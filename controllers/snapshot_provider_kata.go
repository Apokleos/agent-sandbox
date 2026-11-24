// Copyright 2026 The Kubernetes Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controllers

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
)

const kataSnapshotProviderID = "kata-podcheckpoint/v1"

type kataSnapshotProvider struct{ client dynamic.Interface }

var _ SnapshotProvider = kataSnapshotProvider{}

func (r *SandboxReconciler) snapshotProvider() SnapshotProvider {
	return kataSnapshotProvider{client: r.DynamicClient}
}

func (p kataSnapshotProvider) ID() string { return kataSnapshotProviderID }

func (p kataSnapshotProvider) Capabilities(_ context.Context, pod *corev1.Pod) (SnapshotCapabilities, error) {
	result := SnapshotCapabilities{MemoryState: SnapshotUnverified, ProcessState: SnapshotUnverified, WritableRootFS: SnapshotUnverified, RestoreRequired: SnapshotUnverified, SameNodeRequired: SnapshotSupported}
	if pod == nil || pod.Spec.RuntimeClassName == nil || pod.Spec.HostNetwork || pod.Spec.HostPID || pod.Spec.HostIPC || len(pod.Spec.Volumes) != 0 || pod.Spec.AutomountServiceAccountToken == nil || *pod.Spec.AutomountServiceAccountToken {
		return result, nil
	}
	for _, container := range append(append([]corev1.Container{}, pod.Spec.Containers...), pod.Spec.InitContainers...) {
		if len(container.VolumeMounts) != 0 || len(container.VolumeDevices) != 0 || container.Stdin || container.TTY {
			return result, nil
		}
		for _, resources := range []corev1.ResourceList{container.Resources.Requests, container.Resources.Limits} {
			for name := range resources {
				if strings.Contains(string(name), "/") {
					return result, nil
				}
			}
		}
	}
	// Core must also enforce its explicitly configured RuntimeClass allow-list
	// and supported profile. This describes only that restricted integration.
	result.MemoryState, result.ProcessState, result.WritableRootFS, result.RestoreRequired = SnapshotSupported, SnapshotSupported, SnapshotSupported, SnapshotSupported
	return result, nil
}

func kataSnapshotRef(obj *unstructured.Unstructured) SnapshotObjectRef {
	return SnapshotObjectRef{Resource: podCheckpointGVR, Kind: "PodCheckpoint", Namespace: obj.GetNamespace(), Name: obj.GetName(), UID: obj.GetUID()}
}

func checkpointReadyCondition(checkpoint *unstructured.Unstructured) (status string, reason string, message string) {
	conditions, _, _ := unstructured.NestedSlice(checkpoint.Object, "status", "conditions")
	for _, item := range conditions {
		condition, ok := item.(map[string]any)
		if !ok || condition["type"] != "Ready" {
			continue
		}
		status, _ = condition["status"].(string)
		reason, _ = condition["reason"].(string)
		message, _ = condition["message"].(string)
		return status, reason, message
	}
	return "Unknown", "CheckpointPending", "PodCheckpoint has not reported readiness"
}

func observeKataCheckpoint(obj *unstructured.Unstructured) (SnapshotCheckpointObservation, error) {
	ref := kataSnapshotRef(obj)
	evidence := SnapshotObjectEvidence{Ref: ref, ControllerOwner: metav1.GetControllerOf(obj), TemplateDigest: obj.GetAnnotations()[checkpointTemplateHash], DeletionTimestamp: obj.GetDeletionTimestamp()}
	observation := SnapshotCheckpointObservation{Binding: SnapshotBinding{ProviderID: kataSnapshotProviderID, OperationID: strings.TrimPrefix(obj.GetName(), "as-ckpt-"), Request: ref, Artifact: &ref}, Request: evidence, Artifact: &evidence, State: SnapshotUnknown, Reason: "CheckpointPending", Message: "PodCheckpoint has not reported readiness"}
	name, _, err := unstructured.NestedString(obj.Object, "spec", "sourcePod", "name")
	if err != nil {
		return observation, err
	}
	uid, _, err := unstructured.NestedString(obj.Object, "spec", "sourcePod", "uid")
	if err != nil {
		return observation, err
	}
	node, _, err := unstructured.NestedString(obj.Object, "status", "nodeName")
	if err != nil {
		return observation, err
	}
	_, found, err := unstructured.NestedMap(obj.Object, "status", "checkpointedPodTemplate", "spec")
	if err != nil {
		return observation, err
	}
	observation.ObservedSourceName, observation.ObservedSourceUID, observation.SourceNode, observation.HasCheckpointedPodSpec = name, types.UID(uid), node, found
	status, reason, message := checkpointReadyCondition(obj)
	observation.Reason, observation.Message = reason, message
	switch status {
	case "True":
		observation.State = SnapshotReady
	case "False":
		observation.State = SnapshotPending
		if reason == "CheckpointFailed" || reason == "SourcePodReplaced" {
			observation.State = SnapshotFailed
		}
	}
	if obj.GetDeletionTimestamp() != nil {
		observation.State = SnapshotUnknown
		observation.Reason = "CheckpointTerminating"
	}
	return observation, nil
}

func (p kataSnapshotProvider) boundCheckpoint(ctx context.Context, binding SnapshotBinding, requireArtifactUID bool) (*unstructured.Unstructured, error) {
	if binding.ProviderID != kataSnapshotProviderID || binding.Request.Resource != podCheckpointGVR || binding.Request.Kind != "PodCheckpoint" || binding.Request.Name == "" {
		return nil, fmt.Errorf("invalid Kata snapshot binding")
	}
	if requireArtifactUID && (binding.Artifact == nil || binding.Artifact.UID == "") {
		return nil, fmt.Errorf("kata restore requires a persisted artifact UID")
	}
	obj, err := p.client.Resource(podCheckpointGVR).Namespace(binding.Request.Namespace).Get(ctx, binding.Request.Name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if binding.Request.UID != "" && obj.GetUID() != binding.Request.UID {
		return nil, fmt.Errorf("PodCheckpoint %q was replaced", obj.GetName())
	}
	if binding.Artifact != nil {
		ref := binding.Artifact
		if ref.Resource != podCheckpointGVR || ref.Kind != "PodCheckpoint" || ref.Namespace != obj.GetNamespace() || ref.Name != obj.GetName() || (ref.UID != "" && ref.UID != obj.GetUID()) {
			return nil, fmt.Errorf("kata snapshot artifact identity changed")
		}
	}
	return obj, nil
}

func (p kataSnapshotProvider) EnsureCheckpoint(ctx context.Context, input SnapshotCheckpointInput) (SnapshotCheckpointObservation, error) {
	metadata, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&input.RequestMeta)
	if err != nil {
		return SnapshotCheckpointObservation{}, err
	}
	if input.SourcePod == nil {
		return SnapshotCheckpointObservation{}, fmt.Errorf("missing source Pod")
	}
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": podCheckpointGVR.GroupVersion().String(), "kind": "PodCheckpoint", "metadata": metadata,
		"spec": map[string]any{"sourcePod": map[string]any{"name": input.SourcePod.Name, "uid": string(input.SourcePod.UID)}, "timeoutSeconds": checkpointTimeoutSeconds},
	}}
	obj.SetName(input.RequestName)
	annotations := obj.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[checkpointTemplateHash] = input.TemplateDigest
	obj.SetAnnotations(annotations)
	api := p.client.Resource(podCheckpointGVR).Namespace(obj.GetNamespace())
	created, err := api.Create(ctx, obj, metav1.CreateOptions{FieldManager: sandboxControllerFieldOwner})
	if apierrors.IsAlreadyExists(err) {
		created, err = api.Get(ctx, obj.GetName(), metav1.GetOptions{})
	}
	if err != nil {
		return SnapshotCheckpointObservation{}, err
	}
	// Core validates ownership, source UID and digest even after AlreadyExists.
	return observeKataCheckpoint(created)
}

func (p kataSnapshotProvider) ObserveCheckpoint(ctx context.Context, binding SnapshotBinding) (SnapshotCheckpointObservation, error) {
	obj, err := p.boundCheckpoint(ctx, binding, false)
	if err != nil {
		return SnapshotCheckpointObservation{}, err
	}
	return observeKataCheckpoint(obj)
}

func (p kataSnapshotProvider) ListCheckpoints(ctx context.Context, namespace string, options metav1.ListOptions) ([]SnapshotCheckpointObservation, error) {
	list, err := p.client.Resource(podCheckpointGVR).Namespace(namespace).List(ctx, options)
	if err != nil {
		return nil, err
	}
	result := make([]SnapshotCheckpointObservation, 0, len(list.Items))
	for i := range list.Items {
		observation, err := observeKataCheckpoint(&list.Items[i])
		if err != nil {
			return nil, err
		}
		result = append(result, observation)
	}
	return result, nil
}

func (p kataSnapshotProvider) PrepareRestore(ctx context.Context, binding SnapshotBinding) (SnapshotRestorePodPlan, error) {
	obj, err := p.boundCheckpoint(ctx, binding, true)
	if err != nil {
		return SnapshotRestorePodPlan{}, err
	}
	observation, err := observeKataCheckpoint(obj)
	if err != nil {
		return SnapshotRestorePodPlan{}, err
	}
	if observation.State != SnapshotReady || !observation.HasCheckpointedPodSpec {
		return SnapshotRestorePodPlan{}, fmt.Errorf("checkpoint is not ready with a valid PodSpec")
	}
	template, _, err := unstructured.NestedMap(obj.Object, "status", "checkpointedPodTemplate")
	if err != nil {
		return SnapshotRestorePodPlan{}, err
	}
	spec, _, err := unstructured.NestedMap(template, "spec")
	if err != nil {
		return SnapshotRestorePodPlan{}, err
	}
	spec["restoreFrom"] = map[string]any{"name": obj.GetName()}
	// Placement is supplied by restore admission, not a client nodeName.
	delete(spec, "nodeName")
	metadata, _, err := unstructured.NestedMap(template, "metadata")
	if err != nil {
		return SnapshotRestorePodPlan{}, err
	}
	if metadata == nil {
		metadata = map[string]any{}
	}
	pod := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": metadata, "spec": spec}}
	return SnapshotRestorePodPlan{Pod: pod, Artifact: kataSnapshotRef(obj), SourceNode: observation.SourceNode}, nil
}

func (p kataSnapshotProvider) ObserveRestore(ctx context.Context, binding SnapshotBinding, target SnapshotObjectRef) (SnapshotRestoreProof, error) {
	obj, err := p.boundCheckpoint(ctx, binding, true)
	if err != nil {
		return SnapshotRestoreProof{}, err
	}
	observation, err := observeKataCheckpoint(obj)
	if err != nil {
		return SnapshotRestoreProof{}, err
	}
	artifact := kataSnapshotRef(obj)
	proof := SnapshotRestoreProof{State: SnapshotUnknown, Artifact: &artifact, Reason: "RestoreSelectionUnverified"}
	if observation.State != SnapshotReady {
		return proof, nil
	}
	if target.Resource != podGVR || target.Kind != "Pod" || target.Namespace != binding.Request.Namespace {
		return proof, fmt.Errorf("invalid restore Pod reference")
	}
	pod, err := p.client.Resource(podGVR).Namespace(target.Namespace).Get(ctx, target.Name, metav1.GetOptions{})
	if err != nil {
		return proof, err
	}
	proof.PodUID = pod.GetUID()
	if target.UID != "" && target.UID != pod.GetUID() {
		return proof, fmt.Errorf("restore Pod was replaced")
	}
	selection, _, err := unstructured.NestedString(pod.Object, "spec", "restoreFrom", "name")
	if err != nil {
		return proof, err
	}
	if selection == artifact.Name && pod.GetDeletionTimestamp() == nil {
		proof.State, proof.Reason = SnapshotReady, "RestoreSelectionMatched"
		proof.Message = "Fresh artifact UID and Pod restoreFrom selection match; core must verify owner, attempt and readiness"
	}
	return proof, nil
}

func (p kataSnapshotProvider) DeleteCheckpoint(ctx context.Context, binding SnapshotBinding) (SnapshotCleanupObservation, error) {
	result := SnapshotCleanupObservation{AssetDisposition: SnapshotAssetsRetained, Reason: "RetainPolicy"}
	if binding.Request.UID == "" {
		return result, fmt.Errorf("checkpoint deletion requires a bound UID")
	}
	obj, err := p.boundCheckpoint(ctx, binding, false)
	if apierrors.IsNotFound(err) {
		result.RequestGone, result.ArtifactGone = true, true
		return result, nil
	}
	if err != nil {
		return result, err
	}
	if obj.GetDeletionTimestamp() == nil {
		uid := obj.GetUID()
		err := p.client.Resource(podCheckpointGVR).Namespace(obj.GetNamespace()).Delete(ctx, obj.GetName(), metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}})
		if apierrors.IsNotFound(err) {
			result.RequestGone, result.ArtifactGone = true, true
			return result, nil
		}
		if err != nil {
			return result, err
		}
	}
	// A successful Delete response does not prove that finalizers have finished.
	return result, nil
}
