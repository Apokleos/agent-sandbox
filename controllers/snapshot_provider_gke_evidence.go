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
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// These functions map only fields consumed by the existing Python GKE SDK.
// They do not implement SnapshotProvider and cannot issue API requests. A real
// GKE provider requires evidence the SDK does not currently supply.
const gkeSDKEvidenceProviderID = "gke-sdk-evidence"

var (
	gkeSDKTriggerResource  = schema.GroupVersionResource{Group: "podsnapshot.gke.io", Version: "v1", Resource: "podsnapshotmanualtriggers"}
	gkeSDKSnapshotResource = schema.GroupVersionResource{Group: "podsnapshot.gke.io", Version: "v1", Resource: "podsnapshots"}
)

func gkeSDKObjectEvidence(expected SnapshotObjectRef, resource schema.GroupVersionResource, kind string, object *unstructured.Unstructured) (SnapshotObjectEvidence, error) {
	if expected.Resource != resource || expected.Kind != kind || expected.Namespace == "" || expected.Name == "" {
		return SnapshotObjectEvidence{}, fmt.Errorf("invalid expected %s reference", kind)
	}
	if object == nil {
		return SnapshotObjectEvidence{}, fmt.Errorf("%s object is missing", kind)
	}
	if object.GetAPIVersion() != resource.GroupVersion().String() || object.GetKind() != kind ||
		object.GetNamespace() != expected.Namespace || object.GetName() != expected.Name {
		return SnapshotObjectEvidence{}, fmt.Errorf("observed %s type or namespace/name does not match its reference", kind)
	}
	if expected.UID != "" && object.GetUID() != expected.UID {
		return SnapshotObjectEvidence{}, fmt.Errorf("observed %s UID does not match the bound object", kind)
	}
	// Return observed metadata, never an expected UID masquerading as evidence.
	observed := object.DeepCopy()
	return SnapshotObjectEvidence{
		Ref:               SnapshotObjectRef{Resource: resource, Kind: kind, Namespace: observed.GetNamespace(), Name: observed.GetName(), UID: observed.GetUID()},
		ControllerOwner:   metav1.GetControllerOf(observed),
		DeletionTimestamp: observed.GetDeletionTimestamp(),
	}, nil
}

// observeGKESDKCheckpoint keeps trigger and snapshot identities separate. The
// SDK's misleadingly named snapshot_uid is status.snapshotCreated.name, not UID.
func observeGKESDKCheckpoint(binding SnapshotBinding, trigger, snapshot *unstructured.Unstructured) (SnapshotCheckpointObservation, error) {
	observation := SnapshotCheckpointObservation{State: SnapshotUnknown, Reason: "TriggerEvidenceMissing"}
	if binding.ProviderID != gkeSDKEvidenceProviderID {
		return observation, fmt.Errorf("GKE SDK evidence requires its explicit provider identity")
	}
	request, err := gkeSDKObjectEvidence(binding.Request, gkeSDKTriggerResource, "PodSnapshotManualTrigger", trigger)
	if err != nil {
		return observation, err
	}
	observation.Request = request
	observation.Binding = SnapshotBinding{ProviderID: binding.ProviderID, OperationID: binding.OperationID, Request: request.Ref}
	sourceName, _, err := unstructured.NestedString(trigger.Object, "spec", "targetPod")
	if err != nil {
		return observation, fmt.Errorf("read trigger targetPod: %w", err)
	}
	observation.ObservedSourceName = sourceName
	// targetPod names a Pod. Neither it nor origin-pod annotations prove its UID.
	artifactName, _, err := unstructured.NestedString(trigger.Object, "status", "snapshotCreated", "name")
	if err != nil {
		return observation, fmt.Errorf("read trigger snapshotCreated.name: %w", err)
	}
	if binding.Artifact != nil {
		if binding.Artifact.Resource != gkeSDKSnapshotResource || binding.Artifact.Kind != "PodSnapshot" ||
			binding.Artifact.Namespace != request.Ref.Namespace || binding.Artifact.Name == "" ||
			(artifactName != "" && binding.Artifact.Name != artifactName) {
			return observation, fmt.Errorf("trigger artifact reference does not match the bound PodSnapshot")
		}
	}
	if artifactName != "" {
		ref := SnapshotObjectRef{Resource: gkeSDKSnapshotResource, Kind: "PodSnapshot", Namespace: request.Ref.Namespace, Name: artifactName}
		if binding.Artifact != nil {
			ref.UID = binding.Artifact.UID
		}
		if snapshot != nil {
			artifact, err := gkeSDKObjectEvidence(ref, gkeSDKSnapshotResource, "PodSnapshot", snapshot)
			if err != nil {
				return observation, err
			}
			observation.Artifact = &artifact
			observedRef := artifact.Ref
			observation.Binding.Artifact = &observedRef
		} else {
			// The name is observed, but an absent object supplies no UID evidence.
			ref.UID = ""
			observation.Binding.Artifact = &ref
		}
	} else if snapshot != nil {
		return observation, fmt.Errorf("snapshot object has no trigger snapshotCreated.name association")
	}
	conditions, _, err := unstructured.NestedSlice(trigger.Object, "status", "conditions")
	if err != nil {
		return observation, fmt.Errorf("read trigger conditions: %w", err)
	}
	var triggered map[string]any
	for _, item := range conditions {
		condition, ok := item.(map[string]any)
		if !ok {
			return observation, fmt.Errorf("trigger condition is not an object")
		}
		if condition["type"] != "Triggered" {
			continue
		}
		if triggered != nil {
			observation.Reason = "AmbiguousTriggerConditions"
			return observation, nil
		}
		triggered = condition
	}
	if triggered == nil {
		observation.Reason = "TriggerConditionMissing"
		return observation, nil
	}
	status, _, err := unstructured.NestedString(triggered, "status")
	if err != nil {
		return observation, fmt.Errorf("read Triggered status: %w", err)
	}
	reason, _, err := unstructured.NestedString(triggered, "reason")
	if err != nil {
		return observation, fmt.Errorf("read Triggered reason: %w", err)
	}
	message, _, err := unstructured.NestedString(triggered, "message")
	if err != nil {
		return observation, fmt.Errorf("read Triggered message: %w", err)
	}
	observation.Message = message
	if status == "False" {
		observation.State = SnapshotPending
		observation.Reason = "TriggerPending"
		if reason == "Failed" || reason == "Error" {
			observation.State = SnapshotFailed
			observation.Reason = reason
		}
		return observation, nil
	}
	if status != "True" || reason != "Complete" {
		observation.Reason = "TriggerCompletionUnverified"
		return observation, nil
	}
	if request.Ref.UID == "" || artifactName == "" || observation.Artifact == nil || observation.Artifact.Ref.UID == "" {
		observation.Reason = "SnapshotIdentityUnverified"
		return observation, nil
	}
	observation.Reason = "SourceUIDUnverified"
	observation.Message = "SDK trigger completion and PodSnapshot existence do not prove which source Pod UID was captured"
	// No SDK evidence can authorize deletion of a source Pod or return Ready.
	return observation, nil
}

// observeGKESDKRestore never treats message substring matching as exact restore
// identity. The observed annotation contains a name, not an artifact UID.
func observeGKESDKRestore(binding SnapshotBinding, expectedPod SnapshotObjectRef, pod *corev1.Pod) (SnapshotRestoreProof, error) {
	proof := SnapshotRestoreProof{State: SnapshotUnknown, Reason: "RestoreEvidenceMissing"}
	if binding.ProviderID != gkeSDKEvidenceProviderID || binding.Artifact == nil ||
		binding.Artifact.Resource != gkeSDKSnapshotResource || binding.Artifact.Kind != "PodSnapshot" ||
		binding.Artifact.Namespace == "" || binding.Artifact.Name == "" {
		return proof, fmt.Errorf("restore evidence requires a GKE PodSnapshot reference")
	}
	if expectedPod.Resource != (schema.GroupVersionResource{Version: "v1", Resource: "pods"}) || expectedPod.Kind != "Pod" ||
		expectedPod.Name == "" || expectedPod.Namespace != binding.Artifact.Namespace || pod == nil ||
		pod.Namespace != expectedPod.Namespace || pod.Name != expectedPod.Name ||
		(expectedPod.UID != "" && pod.UID != expectedPod.UID) ||
		(pod.APIVersion != "" && pod.APIVersion != "v1") || (pod.Kind != "" && pod.Kind != "Pod") {
		return proof, fmt.Errorf("restore Pod type, namespace/name or UID does not match its reference")
	}
	proof.PodUID = pod.UID
	name := pod.Annotations["podsnapshot.gke.io/ps-name"]
	if name != "" && name != binding.Artifact.Name {
		return proof, fmt.Errorf("restore Pod annotation refers to another snapshot")
	}
	if name != "" {
		proof.Artifact = &SnapshotObjectRef{Resource: gkeSDKSnapshotResource, Kind: "PodSnapshot", Namespace: pod.Namespace, Name: name}
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type != corev1.PodConditionType("PodRestored") {
			continue
		}
		proof.Message = condition.Message
		proof.Reason = "RestoreIdentityUnverified"
		// The SDK does not define a terminal-failure reason contract here.
		// False is not enough to distinguish pending from failed restoration.
		return proof, nil
	}
	return proof, nil
}
