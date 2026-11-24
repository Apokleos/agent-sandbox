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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	clienttesting "k8s.io/client-go/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"

	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
)

func snapshotProviderFixture() (*sandboxv1beta1.Sandbox, *unstructured.Unstructured) {
	sandbox := checkpointTestSandbox(sandboxv1beta1.SandboxOperatingModeRunning)
	sandbox.Status.CheckpointRestore = &sandboxv1beta1.SandboxCheckpointRestoreStatus{
		CheckpointName: "as-ckpt-contract", CheckpointUID: "checkpoint-uid", SourcePodName: sandbox.Name,
		SourcePodUID: "source-pod-uid", SourceNodeName: "alex-master001", OperationID: "contract", RestoreGeneration: 4,
	}
	return sandbox, readyCheckpoint(sandbox, sandbox.Status.CheckpointRestore.CheckpointName)
}

func TestSnapshotReadyWithoutUIDCannotDeleteSource(t *testing.T) {
	ctx := context.Background()
	sandbox, checkpoint := snapshotProviderFixture()
	sandbox.Spec.OperatingMode = sandboxv1beta1.SandboxOperatingModeHibernated
	sandbox.Status.CheckpointRestore.CheckpointUID = ""
	checkpoint.SetUID("")
	pod := checkpointTestPod(sandbox)
	r := checkpointTestReconciler(sandbox, pod, checkpoint)
	handled, _, err := r.reconcilePodCheckpointRestore(ctx, sandbox, pod)
	require.True(t, handled)
	require.ErrorContains(t, err, "no persisted UID")
	remaining := &corev1.Pod{}
	require.NoError(t, r.Get(ctx, client.ObjectKeyFromObject(pod), remaining))
	require.Equal(t, pod.UID, remaining.UID)
	require.True(t, remaining.DeletionTimestamp.IsZero())
}

func TestKataSnapshotObservationUsesBackendEvidence(t *testing.T) {
	sandbox, checkpoint := snapshotProviderFixture()
	observation, err := observeKataCheckpoint(checkpoint)
	require.NoError(t, err)
	require.Equal(t, SnapshotReady, observation.State)
	require.Equal(t, sandbox.Status.CheckpointRestore.SourcePodUID, observation.ObservedSourceUID)
	require.Equal(t, observation.Binding.Request, *observation.Binding.Artifact)
	require.NotEqual(t, string(observation.Binding.Artifact.UID), observation.Binding.Artifact.Name)
	require.True(t, observation.HasCheckpointedPodSpec)
	checkpoint.Object["status"] = map[string]any{}
	observation, err = observeKataCheckpoint(checkpoint)
	require.NoError(t, err)
	require.Equal(t, SnapshotUnknown, observation.State)
	checkpoint.Object["spec"] = map[string]any{"sourcePod": map[string]any{"name": sandbox.Name}}
	observation, err = observeKataCheckpoint(checkpoint)
	require.NoError(t, err)
	require.Empty(t, observation.ObservedSourceUID)
	require.Error(t, validateSnapshotObservation(sandbox, observation))
}

func TestKataSnapshotNormalizesFailureAndTerminating(t *testing.T) {
	_, checkpoint := snapshotProviderFixture()
	checkpoint.Object["status"].(map[string]any)["conditions"] = []any{map[string]any{"type": "Ready", "status": "False", "reason": "CheckpointFailed", "message": "runtime failed"}}
	observation, err := observeKataCheckpoint(checkpoint)
	require.NoError(t, err)
	require.Equal(t, SnapshotFailed, observation.State)
	require.Equal(t, "runtime failed", observation.Message)
	now := metav1.Now()
	checkpoint.SetDeletionTimestamp(&now)
	observation, err = observeKataCheckpoint(checkpoint)
	require.NoError(t, err)
	require.Equal(t, SnapshotUnknown, observation.State)
	require.NotNil(t, observation.Request.DeletionTimestamp)
}

func TestKataSnapshotRestorePreservesDynamicSpecAndCoreOwnsMetadata(t *testing.T) {
	ctx := context.Background()
	sandbox, checkpoint := snapshotProviderFixture()
	require.NoError(t, unstructured.SetNestedField(checkpoint.Object, map[string]any{"opaque": "preserved"}, "status", "checkpointedPodTemplate", "spec", "x-runtime-extension"))
	require.NoError(t, unstructured.SetNestedField(checkpoint.Object, "source-node", "status", "checkpointedPodTemplate", "spec", "nodeName"))
	r := checkpointTestReconciler(sandbox, nil, checkpoint)
	plan, err := r.snapshotProvider().PrepareRestore(ctx, sandboxSnapshotBinding(sandbox))
	require.NoError(t, err)
	extension, _, err := unstructured.NestedString(plan.Pod.Object, "spec", "x-runtime-extension", "opaque")
	require.NoError(t, err)
	require.Equal(t, "preserved", extension)
	_, present, _ := unstructured.NestedFieldNoCopy(plan.Pod.Object, "spec", "nodeName")
	require.False(t, present)
	plan.Pod.SetName("untrusted-name")
	plan.Pod.SetNamespace("untrusted-namespace")
	plan.Pod.SetUID("untrusted-uid")
	plan.Pod.SetResourceVersion("123")
	plan.Pod.SetOwnerReferences([]metav1.OwnerReference{{Kind: "Other", UID: "untrusted-owner"}})
	plan.Pod.SetLabels(map[string]string{sandboxv1beta1.SandboxNameHashLabel: "untrusted", restoreGenerationLabel: "999", "app": "preserve"})
	pod, err := materializeSnapshotRestorePod(sandbox, plan)
	require.NoError(t, err)
	require.Equal(t, sandbox.Name, pod.GetName())
	require.Equal(t, sandbox.Namespace, pod.GetNamespace())
	require.Empty(t, pod.GetUID())
	require.Empty(t, pod.GetResourceVersion())
	require.Equal(t, sandbox.UID, metav1.GetControllerOf(pod).UID)
	require.Equal(t, NameHash(sandbox.Name), pod.GetLabels()[sandboxv1beta1.SandboxNameHashLabel])
	require.Equal(t, "4", pod.GetLabels()[restoreGenerationLabel])
	require.Equal(t, "preserve", pod.GetLabels()["app"])
	require.Equal(t, "untrusted-name", plan.Pod.GetName(), "core must not mutate the provider plan")
	stored, err := r.DynamicClient.Resource(podCheckpointGVR).Namespace(sandbox.Namespace).Get(ctx, checkpoint.GetName(), metav1.GetOptions{})
	require.NoError(t, err)
	_, present, _ = unstructured.NestedFieldNoCopy(stored.Object, "status", "checkpointedPodTemplate", "spec", "restoreFrom")
	require.False(t, present, "planning must not modify the checkpoint object")
}

func TestKataSnapshotRestoreRejectsReplacedArtifactAndCachedPod(t *testing.T) {
	ctx := context.Background()
	sandbox, checkpoint := snapshotProviderFixture()
	checkpoint.SetUID("replaced-artifact")
	r := checkpointTestReconciler(sandbox, nil, checkpoint)
	_, err := r.snapshotProvider().PrepareRestore(ctx, sandboxSnapshotBinding(sandbox))
	require.ErrorContains(t, err, "replaced")
	_, err = r.snapshotProvider().ObserveRestore(ctx, sandboxSnapshotBinding(sandbox), SnapshotObjectRef{Resource: podGVR, Kind: "Pod", Namespace: sandbox.Namespace, Name: sandbox.Name, UID: "old-pod"})
	require.ErrorContains(t, err, "replaced")
	checkpoint.SetUID(sandbox.Status.CheckpointRestore.CheckpointUID)
	pod := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": map[string]any{"name": sandbox.Name, "namespace": sandbox.Namespace, "uid": "new-pod"}, "spec": map[string]any{"restoreFrom": map[string]any{"name": checkpoint.GetName()}}}}
	pod.SetOwnerReferences([]metav1.OwnerReference{sandboxControllerRef(sandbox.Name)})
	pod.SetLabels(map[string]string{restoreGenerationLabel: "4"})
	r = checkpointTestReconciler(sandbox, nil, checkpoint, pod)
	_, err = r.isRestorePod(ctx, sandbox, sandbox.Name, "old-pod")
	require.ErrorContains(t, err, "replaced")
	matched, err := r.isRestorePod(ctx, sandbox, sandbox.Name, "new-pod")
	require.NoError(t, err)
	require.True(t, matched)
}

func TestKataSnapshotAlreadyExistsValidatesCoreIdentity(t *testing.T) {
	for _, scenario := range []string{"owner", "source", "UID", "template"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			sandbox := checkpointTestSandbox(sandboxv1beta1.SandboxOperatingModeHibernated)
			pod := checkpointTestPod(sandbox)
			operation := checkpointOperationID(sandbox, pod)
			sandbox.Status.CheckpointRestore = &sandboxv1beta1.SandboxCheckpointRestoreStatus{CheckpointName: checkpointName(operation), SourcePodName: pod.Name, SourcePodUID: pod.UID, SourceNodeName: pod.Spec.NodeName, OperationID: operation}
			checkpoint := readyCheckpoint(sandbox, checkpointName(operation))
			switch scenario {
			case "owner":
				checkpoint.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: sandboxv1beta1.GroupVersion.String(), Kind: "Sandbox", Name: sandbox.Name, UID: "foreign", Controller: new(true)}})
			case "source":
				require.NoError(t, unstructured.SetNestedField(checkpoint.Object, "foreign-pod", "spec", "sourcePod", "uid"))
			case "UID":
				sandbox.Status.CheckpointRestore.CheckpointUID = "bound-old-UID"
			case "template":
				checkpoint.SetAnnotations(map[string]string{checkpointTemplateHash: "foreign-template"})
			}
			originalUID := sandbox.Status.CheckpointRestore.CheckpointUID
			r := checkpointTestReconciler(sandbox, pod, checkpoint)
			err := r.createPodCheckpoint(ctx, sandbox, pod, operation)
			require.Error(t, err)
			require.Equal(t, originalUID, sandbox.Status.CheckpointRestore.CheckpointUID)
		})
	}
}

func TestKataSnapshotCleanupWaitsAndRetainsAssets(t *testing.T) {
	ctx := context.Background()
	sandbox, checkpoint := snapshotProviderFixture()
	r := checkpointTestReconciler(sandbox, nil, checkpoint)
	provider := r.snapshotProvider()
	cleanup, err := provider.DeleteCheckpoint(ctx, sandboxSnapshotBinding(sandbox))
	require.NoError(t, err)
	require.False(t, cleanup.RequestGone, "Delete acceptance must not release finalizer")
	require.False(t, cleanup.ArtifactGone)
	require.Equal(t, SnapshotAssetsRetained, cleanup.AssetDisposition)
	cleanup, err = provider.DeleteCheckpoint(ctx, sandboxSnapshotBinding(sandbox))
	require.NoError(t, err)
	require.True(t, cleanup.RequestGone)
	require.True(t, cleanup.ArtifactGone)
	require.Equal(t, SnapshotAssetsRetained, cleanup.AssetDisposition)

	checkpoint.SetFinalizers([]string{"external.test/hold"})
	now := metav1.Now()
	checkpoint.SetDeletionTimestamp(&now)
	r = checkpointTestReconciler(sandbox, nil, checkpoint)
	cleanup, err = r.snapshotProvider().DeleteCheckpoint(ctx, sandboxSnapshotBinding(sandbox))
	require.NoError(t, err)
	require.False(t, cleanup.RequestGone)
}

func TestKataSnapshotDeleteUsesUIDAndPropagatesErrors(t *testing.T) {
	ctx := context.Background()
	sandbox, checkpoint := snapshotProviderFixture()
	r := checkpointTestReconciler(sandbox, nil, checkpoint)
	fakeClient := r.DynamicClient.(interface {
		PrependReactor(string, string, clienttesting.ReactionFunc)
	})
	sentinel := errors.New("backend unavailable")
	fakeClient.PrependReactor("delete", "podcheckpoints", func(action clienttesting.Action) (bool, runtime.Object, error) {
		deleteAction := action.(clienttesting.DeleteAction)
		require.NotNil(t, deleteAction.GetDeleteOptions().Preconditions)
		require.Equal(t, checkpoint.GetUID(), *deleteAction.GetDeleteOptions().Preconditions.UID)
		return true, nil, sentinel
	})
	_, err := r.snapshotProvider().DeleteCheckpoint(ctx, sandboxSnapshotBinding(sandbox))
	require.ErrorIs(t, err, sentinel)
	fakeClient.PrependReactor("list", "podcheckpoints", func(clienttesting.Action) (bool, runtime.Object, error) { return true, nil, sentinel })
	_, err = r.snapshotProvider().ListCheckpoints(ctx, sandbox.Namespace, metav1.ListOptions{})
	require.ErrorIs(t, err, sentinel)
}

func TestKataSnapshotUnsupportedProfileDoesNotClaimCapabilities(t *testing.T) {
	sandbox, _ := snapshotProviderFixture()
	pod := &corev1.Pod{Spec: *sandbox.Spec.PodTemplate.Spec.DeepCopy()}
	provider := kataSnapshotProvider{}
	capabilities, err := provider.Capabilities(context.Background(), pod)
	require.NoError(t, err)
	require.Equal(t, SnapshotSupported, capabilities.MemoryState)
	pod.Spec.Volumes = []corev1.Volume{{Name: "unsupported"}}
	capabilities, err = provider.Capabilities(context.Background(), pod)
	require.NoError(t, err)
	require.Equal(t, SnapshotUnverified, capabilities.MemoryState)
}

func TestSnapshotCoreRejectsUnsafeRestorePlanBeforePodCreation(t *testing.T) {
	ctx := context.Background()
	sandbox, checkpoint := snapshotProviderFixture()
	r := checkpointTestReconciler(sandbox, nil, checkpoint)
	plan, err := r.snapshotProvider().PrepareRestore(ctx, sandboxSnapshotBinding(sandbox))
	require.NoError(t, err)
	for _, scenario := range []string{"missing", "wrong", "nodeName"} {
		t.Run(scenario, func(t *testing.T) {
			candidate := plan
			candidate.Pod = plan.Pod.DeepCopy()
			switch scenario {
			case "missing":
				unstructured.RemoveNestedField(candidate.Pod.Object, "spec", "restoreFrom")
			case "wrong":
				require.NoError(t, unstructured.SetNestedField(candidate.Pod.Object, "other", "spec", "restoreFrom", "name"))
			case "nodeName":
				require.NoError(t, unstructured.SetNestedField(candidate.Pod.Object, "node", "spec", "nodeName"))
			}
			_, err := materializeSnapshotRestorePod(sandbox, candidate)
			require.Error(t, err)
		})
	}
}

func TestSnapshotCoreRejectsDurableRestoredUIDReplacementAndReplay(t *testing.T) {
	ctx := context.Background()
	sandbox, checkpoint := snapshotProviderFixture()
	sandbox.Status.CheckpointRestore.RestoredPodUID = "durably-restored"
	pod := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": map[string]any{"name": sandbox.Name, "namespace": sandbox.Namespace, "uid": "replacement"}, "spec": map[string]any{"restoreFrom": map[string]any{"name": checkpoint.GetName()}}}}
	pod.SetOwnerReferences([]metav1.OwnerReference{sandboxControllerRef(sandbox.Name)})
	pod.SetLabels(map[string]string{restoreGenerationLabel: "4"})
	r := checkpointTestReconciler(sandbox, nil, checkpoint, pod)
	_, err := r.isRestorePod(ctx, sandbox, sandbox.Name, "replacement")
	require.ErrorContains(t, err, "durably bound")
	handled, _, err := r.reconcilePodCheckpointRestore(ctx, sandbox, nil)
	require.True(t, handled)
	require.ErrorContains(t, err, "replay")
	for _, action := range r.DynamicClient.(interface{ Actions() []clienttesting.Action }).Actions() {
		require.False(t, action.GetVerb() == "create" && action.GetResource() == podGVR, "missing durable Pod must not create a replacement")
	}
}
