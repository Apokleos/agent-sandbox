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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
)

const (
	checkpointRestoreFinalizer = "agents.x-k8s.io/checkpoint-restore-cleanup"
	checkpointTimeoutSeconds   = int64(300)
	restoreTimeout             = 10 * time.Minute
	restoreGenerationLabel     = "agents.x-k8s.io/restore-generation"
	checkpointTemplateHash     = "agents.x-k8s.io/checkpoint-template-hash"
)

var (
	podCheckpointGVR = schema.GroupVersionResource{Group: "checkpoint.k8s.io", Version: "v1alpha1", Resource: "podcheckpoints"}
	podGVR           = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}
)

func kataCheckpointObject() *unstructured.Unstructured {
	checkpoint := &unstructured.Unstructured{}
	checkpoint.SetGroupVersionKind(podCheckpointGVR.GroupVersion().WithKind("PodCheckpoint"))
	return checkpoint
}

func checkpointOperationID(sandbox *sandboxv1beta1.Sandbox, pod *corev1.Pod) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{sandbox.Namespace, sandbox.Name, string(sandbox.UID), string(pod.UID)}, "/")))
	return hex.EncodeToString(sum[:10])
}

func checkpointName(operationID string) string { return "as-ckpt-" + operationID }

func sandboxCheckpointTemplateHash(sandbox *sandboxv1beta1.Sandbox) (string, error) {
	template, err := json.Marshal(sandbox.Spec.PodTemplate)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(template)
	return hex.EncodeToString(sum[:]), nil
}

func setCheckpointCondition(sandbox *sandboxv1beta1.Sandbox, conditionType sandboxv1beta1.ConditionType, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&sandbox.Status.Conditions, metav1.Condition{
		Type:               string(conditionType),
		Status:             status,
		ObservedGeneration: sandbox.Generation,
		Reason:             reason,
		Message:            message,
	})
}

func resetCheckpointConditions(sandbox *sandboxv1beta1.Sandbox, reason, message string) {
	setCheckpointCondition(sandbox, sandboxv1beta1.SandboxConditionCheckpointReady, metav1.ConditionFalse, reason, message)
	setCheckpointCondition(sandbox, sandboxv1beta1.SandboxConditionHibernated, metav1.ConditionFalse, reason, message)
	setCheckpointCondition(sandbox, sandboxv1beta1.SandboxConditionRestoreReady, metav1.ConditionFalse, reason, message)
}

func checkpointRestoreUnsupported(sandbox *sandboxv1beta1.Sandbox) bool {
	condition := meta.FindStatusCondition(sandbox.Status.Conditions, string(sandboxv1beta1.SandboxConditionCheckpointRestoreSupported))
	return condition != nil && condition.Status == metav1.ConditionFalse && condition.Reason == "Unsupported" && condition.ObservedGeneration == sandbox.Generation
}

func podReady(pod *corev1.Pod) bool {
	if pod == nil || pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func (r *SandboxReconciler) validateCheckpointRestoreProfile(sandbox *sandboxv1beta1.Sandbox) error {
	if !r.EnablePodCheckpointRestore {
		return fmt.Errorf("controller flag --enable-pod-checkpoint-restore is disabled")
	}
	if r.DynamicClient == nil {
		return fmt.Errorf("dynamic Kubernetes client is not configured")
	}
	spec := &sandbox.Spec.PodTemplate.Spec
	if spec.RuntimeClassName == nil || *spec.RuntimeClassName == "" {
		return fmt.Errorf("podTemplate.spec.runtimeClassName is required")
	}
	if _, allowed := r.PodCheckpointRuntimeClasses[*spec.RuntimeClassName]; !allowed {
		return fmt.Errorf("runtimeClassName %q is not in the Pod checkpoint/restore allow-list", *spec.RuntimeClassName)
	}
	if spec.HostNetwork || spec.HostPID || spec.HostIPC {
		return fmt.Errorf("hostNetwork, hostPID and hostIPC must be false")
	}
	if len(spec.Volumes) != 0 || len(sandbox.Spec.VolumeClaimTemplates) != 0 {
		return fmt.Errorf("volumes and volumeClaimTemplates are not supported by the alpha checkpoint/restore profile")
	}
	if spec.AutomountServiceAccountToken == nil || *spec.AutomountServiceAccountToken {
		return fmt.Errorf("automountServiceAccountToken must be explicitly disabled")
	}
	for _, container := range spec.Containers {
		if len(container.VolumeMounts) != 0 || len(container.VolumeDevices) != 0 || container.Stdin || container.TTY {
			return fmt.Errorf("container %q uses unsupported volumes, stdin, or TTY", container.Name)
		}
	}
	for _, container := range spec.InitContainers {
		if len(container.VolumeMounts) != 0 || len(container.VolumeDevices) != 0 || container.Stdin || container.TTY {
			return fmt.Errorf("init container %q uses unsupported volumes, stdin, or TTY", container.Name)
		}
	}
	for _, containers := range [][]corev1.Container{spec.Containers, spec.InitContainers} {
		for _, container := range containers {
			for _, resources := range []corev1.ResourceList{container.Resources.Requests, container.Resources.Limits} {
				for name := range resources {
					if strings.Contains(string(name), "/") {
						return fmt.Errorf("extended resource %q is unsupported by the alpha checkpoint/restore profile", name)
					}
				}
			}
		}
	}
	// Capabilities currently perform no API I/O; validate the full Sandbox
	// template rather than the deliberately transformed informer PodSpec.
	capabilities, err := r.snapshotProvider().Capabilities(context.Background(), &corev1.Pod{Spec: *spec.DeepCopy()})
	if err != nil {
		return err
	}
	if capabilities.MemoryState != SnapshotSupported || capabilities.ProcessState != SnapshotSupported || capabilities.WritableRootFS != SnapshotSupported || capabilities.RestoreRequired != SnapshotSupported || capabilities.SameNodeRequired != SnapshotSupported {
		return fmt.Errorf("snapshot provider cannot establish the required checkpoint/restore capabilities")
	}
	return nil
}

func (r *SandboxReconciler) createPodCheckpoint(ctx context.Context, sandbox *sandboxv1beta1.Sandbox, pod *corev1.Pod, operationID string) error {
	templateHash, err := sandboxCheckpointTemplateHash(sandbox)
	if err != nil {
		return err
	}
	controller, blockOwnerDeletion := true, true
	input := SnapshotCheckpointInput{OperationID: operationID, RequestName: checkpointName(operationID), SourcePod: pod, TemplateDigest: templateHash,
		RequestMeta: metav1.ObjectMeta{Namespace: sandbox.Namespace, Labels: map[string]string{sandboxv1beta1.SandboxNameHashLabel: NameHash(sandbox.Name)},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: sandboxv1beta1.GroupVersion.String(), Kind: "Sandbox", Name: sandbox.Name, UID: sandbox.UID, Controller: &controller, BlockOwnerDeletion: &blockOwnerDeletion}}}}
	created, err := r.snapshotProvider().EnsureCheckpoint(ctx, input)
	if err != nil {
		return fmt.Errorf("create checkpoint %q: %w", input.RequestName, err)
	}
	if err := validateSnapshotObservation(sandbox, created); err != nil {
		return err
	}
	sandbox.Status.CheckpointRestore.CheckpointUID = created.Request.Ref.UID
	return nil
}

// Legacy durable fields map to both references because Kata's request and
// artifact are one object. This mapping must not be reused for a new backend.
func sandboxSnapshotBinding(sandbox *sandboxv1beta1.Sandbox) SnapshotBinding {
	state := sandbox.Status.CheckpointRestore
	ref := SnapshotObjectRef{Resource: podCheckpointGVR, Kind: "PodCheckpoint", Namespace: sandbox.Namespace, Name: state.CheckpointName, UID: state.CheckpointUID}
	return SnapshotBinding{ProviderID: kataSnapshotProviderID, OperationID: state.OperationID, Request: ref, Artifact: &ref}
}

func snapshotOwnedBySandbox(evidence SnapshotObjectEvidence, sandbox *sandboxv1beta1.Sandbox) bool {
	owner := evidence.ControllerOwner
	return owner != nil && owner.APIVersion == sandboxv1beta1.GroupVersion.String() && owner.Kind == "Sandbox" && owner.Name == sandbox.Name && owner.UID == sandbox.UID
}

func validateSnapshotObservation(sandbox *sandboxv1beta1.Sandbox, checkpoint SnapshotCheckpointObservation) error {
	state := sandbox.Status.CheckpointRestore
	if checkpoint.Binding.ProviderID != kataSnapshotProviderID || checkpoint.Request.Ref.Resource != podCheckpointGVR || checkpoint.Request.Ref.Kind != "PodCheckpoint" ||
		checkpoint.Request.Ref.Namespace != sandbox.Namespace || checkpoint.Request.Ref.Name != state.CheckpointName ||
		!snapshotOwnedBySandbox(checkpoint.Request, sandbox) || checkpoint.Artifact == nil || !snapshotOwnedBySandbox(*checkpoint.Artifact, sandbox) ||
		checkpoint.ObservedSourceName != state.SourcePodName || checkpoint.ObservedSourceUID == "" || checkpoint.ObservedSourceUID != state.SourcePodUID {
		return fmt.Errorf("checkpoint %q conflicts with the active Sandbox checkpoint identity", checkpoint.Request.Ref.Name)
	}
	if state.CheckpointUID != "" && (checkpoint.Request.Ref.UID != state.CheckpointUID || checkpoint.Artifact.Ref.UID != state.CheckpointUID) {
		return fmt.Errorf("checkpoint %q was replaced", checkpoint.Request.Ref.Name)
	}
	if expected := checkpoint.Request.TemplateDigest; expected != "" {
		current, err := sandboxCheckpointTemplateHash(sandbox)
		if err != nil {
			return err
		}
		if current != expected {
			err := fmt.Errorf("sandbox PodTemplate changed after checkpoint %q", checkpoint.Request.Ref.Name)
			setCheckpointCondition(sandbox, sandboxv1beta1.SandboxConditionRestoreReady, metav1.ConditionFalse, "SpecDrifted", err.Error())
			return err
		}
	}
	return nil
}

func (r *SandboxReconciler) getOwnedPodCheckpoint(ctx context.Context, sandbox *sandboxv1beta1.Sandbox) (*SnapshotCheckpointObservation, error) {
	checkpoint, err := r.snapshotProvider().ObserveCheckpoint(ctx, sandboxSnapshotBinding(sandbox))
	if err != nil {
		return nil, err
	}
	if err := validateSnapshotObservation(sandbox, checkpoint); err != nil {
		return nil, err
	}
	return &checkpoint, nil
}

func isOwnedBySandboxCheckpoint(checkpoint *unstructured.Unstructured, sandbox *sandboxv1beta1.Sandbox) bool {
	owner := metav1.GetControllerOf(checkpoint)
	return owner != nil && owner.APIVersion == sandboxv1beta1.GroupVersion.String() &&
		owner.Kind == "Sandbox" && owner.Name == sandbox.Name && owner.UID == sandbox.UID
}

func (r *SandboxReconciler) isRestorePod(ctx context.Context, sandbox *sandboxv1beta1.Sandbox, podName string, expectedUID types.UID) (bool, error) {
	// Recheck core evidence as well as the provider's fresh artifact UID read.
	if _, err := r.getOwnedPodCheckpoint(ctx, sandbox); err != nil {
		return false, err
	}
	pod, err := r.DynamicClient.Resource(podGVR).Namespace(sandbox.Namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return false, err
	}
	if expectedUID != "" && pod.GetUID() != expectedUID {
		return false, fmt.Errorf("restore Pod was replaced since the controller read")
	}
	state := sandbox.Status.CheckpointRestore
	if state.RestoredPodUID != "" && pod.GetUID() != state.RestoredPodUID {
		return false, fmt.Errorf("restore Pod differs from the durably bound restored UID")
	}
	proof, err := r.snapshotProvider().ObserveRestore(ctx, sandboxSnapshotBinding(sandbox), SnapshotObjectRef{Resource: podGVR, Kind: "Pod", Namespace: sandbox.Namespace, Name: podName, UID: pod.GetUID()})
	if err != nil {
		return false, err
	}
	attempt := pod.GetLabels()[restoreGenerationLabel]
	return proof.State == SnapshotReady && proof.PodUID == pod.GetUID() && proof.Artifact != nil && proof.Artifact.UID == state.CheckpointUID &&
		isOwnedBySandboxCheckpoint(pod, sandbox) &&
		(state.RestoreGeneration == 0 || attempt == strconv.FormatInt(state.RestoreGeneration, 10)), nil
}

func snapshotReadyCondition(checkpoint *SnapshotCheckpointObservation) (status, reason, message string) {
	status = string(metav1.ConditionUnknown)
	switch checkpoint.State {
	case SnapshotReady:
		status = string(metav1.ConditionTrue)
	case SnapshotPending, SnapshotFailed:
		status = string(metav1.ConditionFalse)
	}
	return status, checkpoint.Reason, checkpoint.Message
}

// materializeSnapshotRestorePod is core's ownership boundary. Provider metadata
// cannot select another Pod name, namespace, owner, tracking label or attempt.
func materializeSnapshotRestorePod(sandbox *sandboxv1beta1.Sandbox, plan SnapshotRestorePodPlan) (*unstructured.Unstructured, error) {
	state := sandbox.Status.CheckpointRestore
	if plan.Pod == nil || plan.Artifact.UID == "" || plan.Artifact.UID != state.CheckpointUID || plan.Artifact.Name != state.CheckpointName ||
		plan.Artifact.Namespace != sandbox.Namespace || plan.Artifact.Resource != podCheckpointGVR || plan.Artifact.Kind != "PodCheckpoint" ||
		plan.SourceNode == "" || plan.SourceNode != state.SourceNodeName {
		return nil, fmt.Errorf("restore plan conflicts with the bound snapshot")
	}
	restorePod := plan.Pod.DeepCopy()
	spec, found, err := unstructured.NestedMap(restorePod.Object, "spec")
	if err != nil || !found {
		return nil, fmt.Errorf("restore plan has no valid PodSpec")
	}
	selection, selected, err := unstructured.NestedString(spec, "restoreFrom", "name")
	if err != nil || !selected || selection != state.CheckpointName {
		return nil, fmt.Errorf("restore plan does not select the bound checkpoint; refusing cold creation")
	}
	if _, supplied := spec["nodeName"]; supplied {
		return nil, fmt.Errorf("restore plan must not supply nodeName")
	}
	metadata, _, err := unstructured.NestedMap(restorePod.Object, "metadata")
	if err != nil {
		return nil, err
	}
	metadata = map[string]any{
		"name":        sandbox.Status.CheckpointRestore.SourcePodName,
		"namespace":   sandbox.Namespace,
		"labels":      metadata["labels"],
		"annotations": metadata["annotations"],
	}
	metadata["ownerReferences"] = []any{map[string]any{
		"apiVersion":         "agents.x-k8s.io/v1beta1",
		"kind":               "Sandbox",
		"name":               sandbox.Name,
		"uid":                string(sandbox.UID),
		"controller":         true,
		"blockOwnerDeletion": true,
	}}

	restorePod.Object = map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": metadata, "spec": spec}
	labels := restorePod.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	labels[sandboxv1beta1.SandboxNameHashLabel] = NameHash(sandbox.Name)
	if attempt := sandbox.Status.CheckpointRestore.RestoreGeneration; attempt != 0 {
		labels[restoreGenerationLabel] = strconv.FormatInt(attempt, 10)
	} else {
		delete(labels, restoreGenerationLabel)
	}
	restorePod.SetLabels(labels)
	return restorePod, nil
}

func (r *SandboxReconciler) createRestorePod(ctx context.Context, sandbox *sandboxv1beta1.Sandbox, _ *SnapshotCheckpointObservation) error {
	if _, err := r.getOwnedPodCheckpoint(ctx, sandbox); err != nil {
		return err
	}
	plan, err := r.snapshotProvider().PrepareRestore(ctx, sandboxSnapshotBinding(sandbox))
	if err != nil {
		return err
	}
	restorePod, err := materializeSnapshotRestorePod(sandbox, plan)
	if err != nil {
		return err
	}
	_, err = r.DynamicClient.Resource(podGVR).Namespace(sandbox.Namespace).Create(ctx, restorePod, metav1.CreateOptions{FieldManager: sandboxControllerFieldOwner})
	if apierrors.IsAlreadyExists(err) {
		matches, lookupErr := r.isRestorePod(ctx, sandbox, restorePod.GetName(), "")
		if lookupErr != nil {
			return lookupErr
		}
		if !matches {
			return fmt.Errorf("existing Pod %q conflicts with the active Sandbox restore", restorePod.GetName())
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("create restore Pod %q: %w", restorePod.GetName(), err)
	}
	return nil
}

func (r *SandboxReconciler) beginCheckpointCycle(ctx context.Context, sandbox *sandboxv1beta1.Sandbox, pod *corev1.Pod) error {
	operationID := checkpointOperationID(sandbox, pod)
	now := metav1.Now()
	sandbox.Status.CheckpointRestore = &sandboxv1beta1.SandboxCheckpointRestoreStatus{
		CheckpointName: checkpointName(operationID),
		SourcePodName:  pod.Name,
		SourcePodUID:   pod.UID,
		SourceNodeName: pod.Spec.NodeName,
		OperationID:    operationID,
		StartedAt:      &now,
	}
	if err := r.createPodCheckpoint(ctx, sandbox, pod, operationID); err != nil {
		return err
	}
	setCheckpointCondition(sandbox, sandboxv1beta1.SandboxConditionRestoreReady, metav1.ConditionFalse, "CheckpointPending", "Restore has not started for the new checkpoint cycle")
	setCheckpointCondition(sandbox, sandboxv1beta1.SandboxConditionCheckpointReady, metav1.ConditionFalse, "CheckpointPending", "PodCheckpoint was created")
	setCheckpointCondition(sandbox, sandboxv1beta1.SandboxConditionHibernated, metav1.ConditionFalse, "CheckpointPending", "Waiting for checkpoint readiness before deleting the source Pod")
	return nil
}

// reconcilePodCheckpointRestore handles the C/R paths before ordinary Pod
// creation. handled=true means the caller must not enter the cold-create path.
func (r *SandboxReconciler) reconcilePodCheckpointRestore(ctx context.Context, sandbox *sandboxv1beta1.Sandbox, pod *corev1.Pod) (handled bool, resultPod *corev1.Pod, err error) {
	desiredHibernated := effectiveOperatingMode(sandbox) == sandboxv1beta1.SandboxOperatingModeHibernated
	state := sandbox.Status.CheckpointRestore
	if state == nil && pod == nil && slices.Contains(sandbox.Finalizers, checkpointRestoreFinalizer) &&
		r.EnablePodCheckpointRestore && r.DynamicClient != nil {
		recovered, err := r.recoverCheckpointBinding(ctx, sandbox)
		if err != nil || recovered {
			return true, nil, err
		}
	}
	if !desiredHibernated && state == nil {
		if slices.Contains(sandbox.Finalizers, checkpointRestoreFinalizer) {
			pending, err := r.deleteOwnedPodCheckpoints(ctx, sandbox)
			if err != nil || pending {
				return true, pod, err
			}
			if err := r.removeCheckpointRestoreFinalizer(ctx, sandbox); err != nil {
				return true, pod, err
			}
			resetCheckpointConditions(sandbox, "CheckpointDiscarded", "No checkpoint cycle is bound; the cleanup finalizer was released")
			return true, pod, nil
		}
		return false, pod, nil
	}

	if err := r.validateCheckpointRestoreProfile(sandbox); err != nil {
		setCheckpointCondition(sandbox, sandboxv1beta1.SandboxConditionCheckpointRestoreSupported, metav1.ConditionFalse, "Unsupported", err.Error())
		return true, pod, nil
	}
	setCheckpointCondition(sandbox, sandboxv1beta1.SandboxConditionCheckpointRestoreSupported, metav1.ConditionTrue, "Supported", "Pod-level checkpoint/restore is enabled for this Sandbox")
	if state != nil && !slices.Contains(sandbox.Finalizers, checkpointRestoreFinalizer) {
		if err := ensureCheckpointRestoreFinalizer(ctx, r.Client, sandbox); err != nil {
			return true, pod, fmt.Errorf("ensure checkpoint/restore finalizer: %w", err)
		}
		return true, pod, nil
	}

	if desiredHibernated {
		if pod == nil {
			if state == nil {
				err := fmt.Errorf("cannot hibernate Sandbox without a source Pod")
				setCheckpointCondition(sandbox, sandboxv1beta1.SandboxConditionHibernated, metav1.ConditionFalse, "SourcePodMissing", err.Error())
				return true, nil, err
			}
			checkpoint, getErr := r.getOwnedPodCheckpoint(ctx, sandbox)
			if getErr != nil {
				return true, nil, fmt.Errorf("get PodCheckpoint %q: %w", state.CheckpointName, getErr)
			}
			ready, reason, message := snapshotReadyCondition(checkpoint)
			if ready != string(metav1.ConditionTrue) {
				return true, nil, fmt.Errorf("source Pod is absent before checkpoint became ready: %s: %s", reason, message)
			}
			setCheckpointCondition(sandbox, sandboxv1beta1.SandboxConditionCheckpointReady, metav1.ConditionTrue, reason, message)
			setCheckpointCondition(sandbox, sandboxv1beta1.SandboxConditionHibernated, metav1.ConditionTrue, "SourcePodDeleted", "Checkpoint is ready and the source Pod is absent")
			return true, nil, nil
		}

		// The Pod informer keeps only NodeName from PodSpec. Validate the
		// source against a full API read before acting on its runtime profile.
		if r.APIReader != nil {
			source := &corev1.Pod{}
			if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(pod), source); err != nil {
				return true, nil, client.IgnoreNotFound(err)
			}
			if source.UID != pod.UID {
				return true, nil, nil
			}
			pod = source
		}
		if !isOwnedBySandbox(pod, sandbox) {
			return true, pod, fmt.Errorf("refusing to checkpoint Pod %q because it is not owned by the Sandbox", pod.Name)
		}
		if owner := metav1.GetControllerOf(sandbox); owner != nil && owner.Kind == "SandboxWarmPool" {
			err := fmt.Errorf("an unclaimed WarmPool Sandbox cannot be checkpointed")
			setCheckpointCondition(sandbox, sandboxv1beta1.SandboxConditionCheckpointReady, metav1.ConditionFalse, "WarmPoolSandboxUnsupported", err.Error())
			return true, pod, err
		}
		if !podReady(pod) || !pod.DeletionTimestamp.IsZero() {
			setCheckpointCondition(sandbox, sandboxv1beta1.SandboxConditionHibernated, metav1.ConditionFalse, "SourcePodNotReady", "Waiting for source Pod readiness or deletion")
			return true, pod, nil
		}
		if pod.Spec.NodeName == "" || pod.Spec.RuntimeClassName == nil ||
			*pod.Spec.RuntimeClassName != *sandbox.Spec.PodTemplate.Spec.RuntimeClassName ||
			pod.Spec.HostNetwork || len(pod.Spec.Volumes) != 0 {
			return true, pod, fmt.Errorf("live source Pod does not match the checkpoint runtime profile")
		}
		if state != nil && state.SourcePodUID != pod.UID && state.RestoredPodUID != pod.UID {
			return true, pod, fmt.Errorf("source Pod UID changed during the active checkpoint operation")
		}
		if state == nil || state.SourcePodUID != pod.UID {
			if !slices.Contains(sandbox.Finalizers, checkpointRestoreFinalizer) {
				if err := ensureCheckpointRestoreFinalizer(ctx, r.Client, sandbox); err != nil {
					return true, pod, fmt.Errorf("ensure checkpoint/restore finalizer: %w", err)
				}
				return true, pod, nil
			}
			if err := r.beginCheckpointCycle(ctx, sandbox, pod); err != nil {
				return true, pod, err
			}
			return true, pod, nil
		}

		checkpoint, getErr := r.getOwnedPodCheckpoint(ctx, sandbox)
		if getErr != nil {
			return true, pod, fmt.Errorf("get PodCheckpoint %q: %w", state.CheckpointName, getErr)
		}
		ready, reason, message := snapshotReadyCondition(checkpoint)
		previous := meta.FindStatusCondition(sandbox.Status.Conditions, string(sandboxv1beta1.SandboxConditionCheckpointReady))
		if previous != nil && previous.Reason == "CheckpointTimedOut" {
			return true, pod, nil
		}
		if checkpoint.State == SnapshotFailed {
			setCheckpointCondition(sandbox, sandboxv1beta1.SandboxConditionCheckpointReady, metav1.ConditionFalse, reason, message)
			return true, pod, fmt.Errorf("checkpoint failed: %s: %s", reason, message)
		}
		if ready != string(metav1.ConditionTrue) {
			if state.StartedAt != nil && time.Since(state.StartedAt.Time) >= time.Duration(checkpointTimeoutSeconds)*time.Second {
				setCheckpointCondition(sandbox, sandboxv1beta1.SandboxConditionCheckpointReady, metav1.ConditionFalse, "CheckpointTimedOut", "Checkpoint deadline exceeded; source Pod remains running")
				return true, pod, nil
			}
			setCheckpointCondition(sandbox, sandboxv1beta1.SandboxConditionCheckpointReady, metav1.ConditionFalse, reason, message)
			return true, pod, nil
		}
		if !checkpoint.HasCheckpointedPodSpec {
			return true, pod, fmt.Errorf("ready checkpoint %q lacks a valid PodSpec", checkpoint.Request.Ref.Name)
		}
		node := checkpoint.SourceNode
		if node == "" || node != state.SourceNodeName {
			return true, pod, fmt.Errorf("ready checkpoint %q does not match the expected source node", checkpoint.Request.Ref.Name)
		}
		if state.CheckpointUID == "" && checkpoint.Request.Ref.UID != "" {
			state.CheckpointUID = checkpoint.Request.Ref.UID
			return true, pod, nil
		}
		if state.CheckpointUID == "" {
			return true, pod, fmt.Errorf("checkpoint has no persisted UID; refusing source deletion")
		}
		setCheckpointCondition(sandbox, sandboxv1beta1.SandboxConditionCheckpointReady, metav1.ConditionTrue, reason, message)
		if pod.DeletionTimestamp.IsZero() {
			if r.APIReader != nil {
				current := &sandboxv1beta1.Sandbox{}
				if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(sandbox), current); err != nil {
					return true, pod, err
				}
				if !checkpointIntentMatches(sandbox, current) {
					return true, pod, nil
				}
			}
			uid := pod.UID
			version := pod.ResourceVersion
			if err := r.Delete(ctx, pod, client.Preconditions{UID: &uid, ResourceVersion: &version}); err != nil && !apierrors.IsNotFound(err) {
				return true, pod, fmt.Errorf("delete checkpointed source Pod: %w", err)
			}
		}
		setCheckpointCondition(sandbox, sandboxv1beta1.SandboxConditionHibernated, metav1.ConditionFalse, "SourcePodTerminating", "Checkpoint is ready; source Pod is terminating")
		return true, pod, nil
	}

	// Running after a completed checkpoint cycle. Reversing the mode before
	// source deletion cancels the unused checkpoint and keeps the live Pod.
	if pod != nil && pod.UID == state.SourcePodUID {
		// The informer may still show a live source after the delete was accepted.
		if r.APIReader != nil {
			current := &corev1.Pod{}
			if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(pod), current); err != nil {
				return true, nil, client.IgnoreNotFound(err)
			}
			if current.UID != state.SourcePodUID || !current.DeletionTimestamp.IsZero() {
				return true, nil, nil
			}
		}
		if !pod.DeletionTimestamp.IsZero() {
			return true, nil, nil
		}
		pending, err := r.deleteOwnedPodCheckpoints(ctx, sandbox)
		if err != nil || pending {
			return true, pod, err
		}
		if err := r.removeCheckpointRestoreFinalizer(ctx, sandbox); err != nil {
			return true, pod, err
		}
		sandbox.Status.CheckpointRestore = nil
		resetCheckpointConditions(sandbox, "CheckpointDiscarded", "The unused checkpoint was deleted and the source Pod remains running")
		return false, pod, nil
	}

	if pod == nil {
		if state.RestoredPodUID != "" {
			return true, nil, fmt.Errorf("durably bound restored Pod is missing; refusing to replay an old checkpoint")
		}
		checkpoint, getErr := r.getOwnedPodCheckpoint(ctx, sandbox)
		if getErr != nil {
			return true, nil, fmt.Errorf("get PodCheckpoint %q for restore: %w", state.CheckpointName, getErr)
		}
		ready, reason, message := snapshotReadyCondition(checkpoint)
		if ready != string(metav1.ConditionTrue) {
			return true, nil, fmt.Errorf("refusing cold fallback because PodCheckpoint is not Ready: %s: %s", reason, message)
		}
		if state.CheckpointUID == "" || state.RestoreGeneration == 0 {
			state.CheckpointUID = checkpoint.Request.Ref.UID
			state.RestoreGeneration = sandbox.Generation
			now := metav1.Now()
			state.RestoreStartTime = &now
			return true, nil, nil
		}
		cond := meta.FindStatusCondition(sandbox.Status.Conditions, string(sandboxv1beta1.SandboxConditionRestoreReady))
		if cond != nil && (cond.Reason == "RestorePodFailed" || cond.Reason == "RestoreTimedOut") {
			hibernated := meta.FindStatusCondition(sandbox.Status.Conditions, string(sandboxv1beta1.SandboxConditionHibernated))
			if hibernated == nil || hibernated.Status != metav1.ConditionTrue ||
				hibernated.ObservedGeneration <= state.RestoreGeneration || sandbox.Generation <= hibernated.ObservedGeneration {
				return true, nil, nil
			}
			state.RestoreGeneration = sandbox.Generation
			now := metav1.Now()
			state.RestoreStartTime = &now
			setCheckpointCondition(sandbox, sandboxv1beta1.SandboxConditionRestoreReady, metav1.ConditionFalse, "RestoreRetryRequested", "A new restore attempt was requested")
			return true, nil, nil
		}
		if restoreAttemptExpired(state) {
			setCheckpointCondition(sandbox, sandboxv1beta1.SandboxConditionRestoreReady, metav1.ConditionFalse, "RestoreTimedOut", "Restore attempt deadline exceeded")
			return true, nil, nil
		}
		if err := r.createRestorePod(ctx, sandbox, checkpoint); err != nil {
			setCheckpointCondition(sandbox, sandboxv1beta1.SandboxConditionRestoreReady, metav1.ConditionFalse, "RestoreCreateFailed", err.Error())
			return true, nil, err
		}
		setCheckpointCondition(sandbox, sandboxv1beta1.SandboxConditionHibernated, metav1.ConditionFalse, "RestoreStarted", "Restore Pod was created")
		setCheckpointCondition(sandbox, sandboxv1beta1.SandboxConditionRestoreReady, metav1.ConditionFalse, "RestoreInProgress", "Waiting for restored Pod readiness")
		return true, nil, nil
	}

	matches, err := r.isRestorePod(ctx, sandbox, pod.Name, pod.UID)
	if err != nil {
		return true, nil, err
	}
	if !matches {
		return true, nil, fmt.Errorf("existing Pod %q conflicts with the active Sandbox restore", pod.Name)
	}
	if state.RestoredPodUID == "" && (pod.Status.Phase == corev1.PodFailed || (!podReady(pod) && restoreAttemptExpired(state))) {
		reason := "RestoreTimedOut"
		if pod.Status.Phase == corev1.PodFailed {
			reason = "RestorePodFailed"
		}
		cond := meta.FindStatusCondition(sandbox.Status.Conditions, string(sandboxv1beta1.SandboxConditionRestoreReady))
		if cond == nil || cond.Reason != reason {
			setCheckpointCondition(sandbox, sandboxv1beta1.SandboxConditionRestoreReady, metav1.ConditionFalse, reason, "Restore attempt failed; checkpoint remains bound")
			return true, nil, nil
		}
		if pod.DeletionTimestamp.IsZero() {
			uid := pod.UID
			if err := client.IgnoreNotFound(r.Delete(ctx, pod, client.Preconditions{UID: &uid})); err != nil {
				return true, nil, err
			}
		}
		return true, nil, nil
	}
	if podReady(pod) {
		sandbox.Status.CheckpointRestore.RestoredPodUID = pod.UID
		setCheckpointCondition(sandbox, sandboxv1beta1.SandboxConditionRestoreReady, metav1.ConditionTrue, "RestoreCompleted", "Restored Pod is Ready")
	} else {
		setCheckpointCondition(sandbox, sandboxv1beta1.SandboxConditionRestoreReady, metav1.ConditionFalse, "RestoreInProgress", "Waiting for restored Pod readiness")
	}
	return false, pod, nil
}

func restoreAttemptExpired(state *sandboxv1beta1.SandboxCheckpointRestoreStatus) bool {
	return state.RestoreStartTime != nil && time.Since(state.RestoreStartTime.Time) >= restoreTimeout
}

func (r *SandboxReconciler) recoverCheckpointBinding(ctx context.Context, sandbox *sandboxv1beta1.Sandbox) (bool, error) {
	list, err := r.snapshotProvider().ListCheckpoints(ctx, sandbox.Namespace, metav1.ListOptions{
		LabelSelector: sandboxv1beta1.SandboxNameHashLabel + "=" + NameHash(sandbox.Name),
	})
	if err != nil {
		return false, err
	}
	var binding *sandboxv1beta1.SandboxCheckpointRestoreStatus
	for i := range list {
		checkpoint := &list[i]
		if !snapshotOwnedBySandbox(checkpoint.Request, sandbox) {
			continue
		}
		if binding != nil {
			return false, fmt.Errorf("multiple owned PodCheckpoints prevent recovery of Sandbox status")
		}
		name, uid, node := checkpoint.ObservedSourceName, checkpoint.ObservedSourceUID, checkpoint.SourceNode
		source := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{UID: uid}}
		operationID := checkpointOperationID(sandbox, source)
		if checkpoint.Request.DeletionTimestamp != nil || checkpoint.Request.Ref.UID == "" || name != resolvePodName(sandbox) ||
			uid == "" || node == "" || checkpoint.Request.Ref.Name != checkpointName(operationID) || checkpoint.State != SnapshotReady || !checkpoint.HasCheckpointedPodSpec {
			return false, fmt.Errorf("owned checkpoint %q cannot prove the missing Sandbox binding", checkpoint.Request.Ref.Name)
		}
		binding = &sandboxv1beta1.SandboxCheckpointRestoreStatus{
			CheckpointName: checkpoint.Request.Ref.Name, CheckpointUID: checkpoint.Request.Ref.UID,
			SourcePodName: name, SourcePodUID: uid, SourceNodeName: node, OperationID: operationID,
		}
		// Validate template/owner evidence before persisting recovered state.
		original := sandbox.Status.CheckpointRestore
		sandbox.Status.CheckpointRestore = binding
		validationErr := validateSnapshotObservation(sandbox, *checkpoint)
		sandbox.Status.CheckpointRestore = original
		if validationErr != nil {
			return false, validationErr
		}
	}
	if binding == nil {
		return false, nil
	}
	sandbox.Status.CheckpointRestore = binding
	return true, nil
}

func (r *SandboxReconciler) deleteCheckpointSandboxPods(ctx context.Context, sandbox *sandboxv1beta1.Sandbox) (bool, error) {
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	var pods corev1.PodList
	if err := reader.List(ctx, &pods, client.InNamespace(sandbox.Namespace), client.MatchingLabels{sandboxv1beta1.SandboxNameHashLabel: NameHash(sandbox.Name)}); err != nil {
		return false, err
	}
	pending := false
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !isOwnedBySandbox(pod, sandbox) {
			continue
		}
		pending = true
		if pod.DeletionTimestamp.IsZero() {
			uid := pod.UID
			if err := client.IgnoreNotFound(r.Delete(ctx, pod, client.Preconditions{UID: &uid})); err != nil {
				return false, err
			}
		}
	}
	return pending, nil
}

func (r *SandboxReconciler) deleteOwnedPodCheckpoints(ctx context.Context, sandbox *sandboxv1beta1.Sandbox) (bool, error) {
	if r.DynamicClient == nil {
		return false, fmt.Errorf("dynamic Kubernetes client is required for checkpoint cleanup")
	}
	list, err := r.snapshotProvider().ListCheckpoints(ctx, sandbox.Namespace, metav1.ListOptions{LabelSelector: sandboxv1beta1.SandboxNameHashLabel + "=" + NameHash(sandbox.Name)})
	if err != nil {
		return false, err
	}
	pending := false
	for i := range list {
		checkpoint := &list[i]
		if !snapshotOwnedBySandbox(checkpoint.Request, sandbox) {
			continue
		}
		if checkpoint.Request.DeletionTimestamp != nil {
			pending = true
			continue
		}
		cleanup, err := r.snapshotProvider().DeleteCheckpoint(ctx, checkpoint.Binding)
		if err != nil {
			return false, err
		}
		if !cleanup.RequestGone || !cleanup.ArtifactGone {
			pending = true
		}
	}
	return pending, nil
}

func (r *SandboxReconciler) removeCheckpointRestoreFinalizer(ctx context.Context, sandbox *sandboxv1beta1.Sandbox) error {
	if !slices.Contains(sandbox.Finalizers, checkpointRestoreFinalizer) {
		return nil
	}
	patch := client.MergeFrom(sandbox.DeepCopy())
	sandbox.Finalizers = slices.DeleteFunc(sandbox.Finalizers, func(item string) bool { return item == checkpointRestoreFinalizer })
	return r.Patch(ctx, sandbox, patch)
}

func ensureCheckpointRestoreFinalizer(ctx context.Context, r client.Client, sandbox *sandboxv1beta1.Sandbox) error {
	if slices.Contains(sandbox.Finalizers, checkpointRestoreFinalizer) {
		return nil
	}
	patch := client.MergeFrom(sandbox.DeepCopy())
	sandbox.Finalizers = append(sandbox.Finalizers, checkpointRestoreFinalizer)
	return r.Patch(ctx, sandbox, patch)
}
