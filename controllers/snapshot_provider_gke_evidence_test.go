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
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Fields mirror snapshot_engine.py's creation manifest and utils.py's
// _get_snapshot_info; constants.py defines the GVRs. Kubernetes metadata is
// supplied separately. No unobserved GKE snapshot spec/status is invented.
func gkeSDKFixture() (SnapshotBinding, *unstructured.Unstructured, *unstructured.Unstructured) {
	trigger := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "podsnapshot.gke.io/v1", "kind": "PodSnapshotManualTrigger",
		"metadata": map[string]any{"namespace": "test", "name": "trigger-request", "uid": "trigger-instance-uid"},
		"spec":     map[string]any{"targetPod": "source-pod"},
		"status": map[string]any{
			"conditions":      []any{map[string]any{"type": "Triggered", "status": "True", "reason": "Complete", "lastTransitionTime": "2023-01-01T00:00:00Z"}},
			"snapshotCreated": map[string]any{"name": "snapshot-uid"},
		},
	}}
	snapshot := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "podsnapshot.gke.io/v1", "kind": "PodSnapshot",
		"metadata": map[string]any{"namespace": "test", "name": "snapshot-uid", "uid": "snapshot-instance-uid"},
	}}
	binding := SnapshotBinding{ProviderID: gkeSDKEvidenceProviderID, OperationID: "operation",
		Request: SnapshotObjectRef{Resource: gkeSDKTriggerResource, Kind: "PodSnapshotManualTrigger", Namespace: "test", Name: "trigger-request", UID: "trigger-instance-uid"}}
	return binding, trigger, snapshot
}

func TestGKESDKCompletionCannotAuthorizeSourceDeletion(t *testing.T) {
	binding, trigger, snapshot := gkeSDKFixture()
	beforeTrigger, beforeSnapshot := trigger.DeepCopy(), snapshot.DeepCopy()
	observed, err := observeGKESDKCheckpoint(binding, trigger, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if observed.State != SnapshotUnknown || observed.Reason != "SourceUIDUnverified" || observed.ObservedSourceUID != "" {
		t.Fatalf("insufficient source evidence authorized checkpoint: %+v", observed)
	}
	if observed.ObservedSourceName != "source-pod" || observed.HasCheckpointedPodSpec {
		t.Fatalf("SDK source fields were fabricated: %+v", observed)
	}
	if observed.Binding.Artifact == nil || observed.Artifact == nil ||
		observed.Binding.Artifact.Name != "snapshot-uid" || observed.Binding.Artifact.UID != "snapshot-instance-uid" ||
		observed.Binding.Request.UID != "trigger-instance-uid" || observed.Binding.Request.Resource == observed.Binding.Artifact.Resource {
		t.Fatalf("trigger/name/artifact UID conflated: %+v", observed)
	}
	if !reflect.DeepEqual(trigger, beforeTrigger) || !reflect.DeepEqual(snapshot, beforeSnapshot) {
		t.Fatal("pure evidence mapping mutated its inputs")
	}
}

func TestGKESDKTriggerStatesNeverPromoteMissingEvidence(t *testing.T) {
	cases := []struct {
		name, status, reason                            string
		missingConditions, missingName, missingSnapshot bool
		want                                            SnapshotState
	}{
		{name: "pending", status: "False", want: SnapshotPending},
		{name: "failed", status: "False", reason: "Failed", want: SnapshotFailed},
		{name: "error", status: "False", reason: "Error", want: SnapshotFailed},
		{name: "unknown condition", status: "Unknown", want: SnapshotUnknown},
		{name: "conditions missing", missingConditions: true, want: SnapshotUnknown},
		{name: "snapshot name missing", status: "True", reason: "Complete", missingName: true, missingSnapshot: true, want: SnapshotUnknown},
		{name: "snapshot not observed", status: "True", reason: "Complete", missingSnapshot: true, want: SnapshotUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			binding, trigger, snapshot := gkeSDKFixture()
			_ = unstructured.SetNestedSlice(trigger.Object, []any{map[string]any{"type": "Triggered", "status": tc.status, "reason": tc.reason, "message": "SDK condition message"}}, "status", "conditions")
			if tc.missingConditions {
				unstructured.RemoveNestedField(trigger.Object, "status", "conditions")
			}
			if tc.missingName {
				unstructured.RemoveNestedField(trigger.Object, "status", "snapshotCreated")
			}
			if tc.missingSnapshot {
				snapshot = nil
			}
			observed, err := observeGKESDKCheckpoint(binding, trigger, snapshot)
			if err != nil || observed.State != tc.want || observed.ObservedSourceUID != "" {
				t.Fatalf("observation=%+v err=%v", observed, err)
			}
		})
	}
}

func TestGKESDKRejectsReplacedOrUnrelatedObjects(t *testing.T) {
	cases := []struct {
		name   string
		change func(*SnapshotBinding, *unstructured.Unstructured, *unstructured.Unstructured)
	}{
		{"request UID replacement", func(_ *SnapshotBinding, tr, _ *unstructured.Unstructured) { tr.SetUID("replacement") }},
		{"request namespace", func(_ *SnapshotBinding, tr, _ *unstructured.Unstructured) { tr.SetNamespace("other") }},
		{"request name", func(_ *SnapshotBinding, tr, _ *unstructured.Unstructured) { tr.SetName("other") }},
		{"request kind", func(_ *SnapshotBinding, tr, _ *unstructured.Unstructured) { tr.SetKind("PodSnapshot") }},
		{"request API version", func(_ *SnapshotBinding, tr, _ *unstructured.Unstructured) { tr.SetAPIVersion("podsnapshot.gke.io/v2") }},
		{"artifact UID replacement", func(b *SnapshotBinding, _, sn *unstructured.Unstructured) {
			b.Artifact = &SnapshotObjectRef{Resource: gkeSDKSnapshotResource, Kind: "PodSnapshot", Namespace: "test", Name: "snapshot-uid", UID: "expected-snapshot-uid"}
			sn.SetUID("replacement")
		}},
		{"artifact namespace", func(_ *SnapshotBinding, _, sn *unstructured.Unstructured) { sn.SetNamespace("other") }},
		{"artifact name", func(_ *SnapshotBinding, _, sn *unstructured.Unstructured) { sn.SetName("other") }},
		{"artifact kind", func(_ *SnapshotBinding, _, sn *unstructured.Unstructured) { sn.SetKind("PodSnapshotManualTrigger") }},
		{"artifact API version", func(_ *SnapshotBinding, _, sn *unstructured.Unstructured) { sn.SetAPIVersion("podsnapshot.gke.io/v2") }},
		{"unassociated snapshot", func(_ *SnapshotBinding, tr, _ *unstructured.Unstructured) {
			unstructured.RemoveNestedField(tr.Object, "status", "snapshotCreated")
		}},
		{"wrong provider", func(b *SnapshotBinding, _, _ *unstructured.Unstructured) { b.ProviderID = "kata" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			binding, trigger, snapshot := gkeSDKFixture()
			tc.change(&binding, trigger, snapshot)
			if observed, err := observeGKESDKCheckpoint(binding, trigger, snapshot); err == nil || observed.State == SnapshotReady {
				t.Fatalf("unrelated evidence accepted: %+v %v", observed, err)
			}
		})
	}
}

func TestGKESDKObservesUIDInsteadOfCopyingExpectedValues(t *testing.T) {
	binding, trigger, snapshot := gkeSDKFixture()
	binding.Request.UID = ""
	observed, err := observeGKESDKCheckpoint(binding, trigger, snapshot)
	if err != nil || observed.Request.Ref.UID != trigger.GetUID() {
		t.Fatalf("actual metadata UID lost: %+v %v", observed, err)
	}
	snapshot.SetUID("")
	observed, err = observeGKESDKCheckpoint(binding, trigger, snapshot)
	if err != nil || observed.State != SnapshotUnknown || observed.Binding.Artifact.UID != "" {
		t.Fatalf("missing UID synthesized: %+v %v", observed, err)
	}
}

func TestGKESDKAmbiguousOrMalformedConditionsFailClosed(t *testing.T) {
	binding, trigger, snapshot := gkeSDKFixture()
	_ = unstructured.SetNestedSlice(trigger.Object, []any{
		map[string]any{"type": "Triggered", "status": "True", "reason": "Complete"},
		map[string]any{"type": "Triggered", "status": "False", "reason": "Failed"},
	}, "status", "conditions")
	observed, err := observeGKESDKCheckpoint(binding, trigger, snapshot)
	if err != nil || observed.State != SnapshotUnknown || observed.Reason != "AmbiguousTriggerConditions" {
		t.Fatalf("ambiguous conditions promoted: %+v %v", observed, err)
	}
	_ = unstructured.SetNestedField(trigger.Object, "not-a-list", "status", "conditions")
	if observed, err := observeGKESDKCheckpoint(binding, trigger, snapshot); err == nil || observed.State == SnapshotReady {
		t.Fatalf("malformed conditions accepted: %+v %v", observed, err)
	}
}

func gkeSDKRestoreFixture() (SnapshotBinding, SnapshotObjectRef, *corev1.Pod) {
	binding, _, _ := gkeSDKFixture()
	binding.Artifact = &SnapshotObjectRef{Resource: gkeSDKSnapshotResource, Kind: "PodSnapshot", Namespace: "test", Name: "snapshot-uid", UID: "snapshot-instance-uid"}
	expected := SnapshotObjectRef{Resource: schema.GroupVersionResource{Version: "v1", Resource: "pods"}, Kind: "Pod", Namespace: "test", Name: "restored", UID: "restored-instance-uid"}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "test", Name: "restored", UID: "restored-instance-uid", Annotations: map[string]string{"podsnapshot.gke.io/ps-name": "snapshot-uid"}},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodConditionType("PodRestored"), Status: corev1.ConditionTrue, Message: "Restored from snapshot snapshot-uid"}}}}
	return binding, expected, pod
}

func TestGKESDKMessageSubstringIsNotRestoreProof(t *testing.T) {
	for _, message := range []string{"Restored from snapshot snapshot-uid", "Restored from snapshot snapshot-uid-extra", "unrelated message"} {
		binding, expected, pod := gkeSDKRestoreFixture()
		pod.Status.Conditions[0].Message = message
		proof, err := observeGKESDKRestore(binding, expected, pod)
		if err != nil || proof.State != SnapshotUnknown || proof.PodUID != pod.UID || proof.Artifact == nil || proof.Artifact.UID != "" {
			t.Fatalf("message %q authorized restore or fabricated UID: %+v %v", message, proof, err)
		}
	}
}

func TestGKESDKRestoreRejectsIdentityChanges(t *testing.T) {
	for _, field := range []string{"namespace", "name", "UID", "annotation", "kind", "API version"} {
		t.Run(field, func(t *testing.T) {
			binding, expected, pod := gkeSDKRestoreFixture()
			switch field {
			case "namespace":
				pod.Namespace = "other"
			case "name":
				pod.Name = "other"
			case "UID":
				pod.UID = "replacement"
			case "annotation":
				pod.Annotations["podsnapshot.gke.io/ps-name"] = "other"
			case "kind":
				pod.Kind = "Sandbox"
			case "API version":
				pod.APIVersion = "v2"
			}
			if proof, err := observeGKESDKRestore(binding, expected, pod); err == nil || proof.State == SnapshotReady {
				t.Fatalf("restore identity accepted: %+v %v", proof, err)
			}
		})
	}
	for _, status := range []corev1.ConditionStatus{corev1.ConditionFalse, corev1.ConditionUnknown} {
		binding, expected, pod := gkeSDKRestoreFixture()
		pod.Status.Conditions[0].Status = status
		proof, err := observeGKESDKRestore(binding, expected, pod)
		if err != nil || proof.State != SnapshotUnknown {
			t.Fatalf("unverified restore promoted: %+v %v", proof, err)
		}
	}
}
