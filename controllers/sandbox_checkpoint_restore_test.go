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
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	ctrl "sigs.k8s.io/controller-runtime"

	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	asmetrics "sigs.k8s.io/agent-sandbox/internal/metrics"
)

func checkpointTestSandbox(mode sandboxv1beta1.SandboxOperatingMode) *sandboxv1beta1.Sandbox {
	runtimeClass := "kata-pod-ckpt"
	automount := false
	return &sandboxv1beta1.Sandbox{
		TypeMeta: metav1.TypeMeta{APIVersion: "agents.x-k8s.io/v1beta1", Kind: "Sandbox"},
		ObjectMeta: metav1.ObjectMeta{
			Name:       "checkpoint-test",
			Namespace:  "default",
			UID:        sandboxUID,
			Generation: 2,
		},
		Spec: sandboxv1beta1.SandboxSpec{
			OperatingMode: mode,
			SandboxBlueprint: sandboxv1beta1.SandboxBlueprint{PodTemplate: sandboxv1beta1.PodTemplate{Spec: corev1.PodSpec{
				RuntimeClassName:             &runtimeClass,
				AutomountServiceAccountToken: &automount,
				Containers: []corev1.Container{{
					Name:    "worker",
					Image:   "registry.k8s.io/busybox:1.36.1",
					Command: []string{"sh", "-c", "sleep 3600"},
				}},
			}}},
		},
	}
}

func checkpointTestPod(sandbox *sandboxv1beta1.Sandbox) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            sandbox.Name,
			Namespace:       sandbox.Namespace,
			UID:             types.UID("source-pod-uid"),
			OwnerReferences: []metav1.OwnerReference{sandboxControllerRef(sandbox.Name)},
		},
		Spec: corev1.PodSpec{NodeName: "alex-master001", RuntimeClassName: sandbox.Spec.PodTemplate.Spec.RuntimeClassName},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{{
				Type: corev1.PodReady, Status: corev1.ConditionTrue,
			}},
		},
	}
}

func readyCheckpoint(sandbox *sandboxv1beta1.Sandbox, name string) *unstructured.Unstructured {
	checkpoint := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "checkpoint.k8s.io/v1alpha1",
		"kind":       "PodCheckpoint",
		"metadata": map[string]any{
			"name": name, "namespace": sandbox.Namespace,
		},
		"spec": map[string]any{"sourcePod": map[string]any{
			"name": sandbox.Status.CheckpointRestore.SourcePodName,
			"uid":  string(sandbox.Status.CheckpointRestore.SourcePodUID),
		}},
		"status": map[string]any{
			"nodeName": "alex-master001",
			"conditions": []any{map[string]any{
				"type": "Ready", "status": "True", "reason": "CheckpointCompleted", "message": "checkpoint completed",
			}},
			"checkpointedPodTemplate": map[string]any{
				"metadata": map[string]any{"labels": map[string]any{sandboxv1beta1.SandboxNameHashLabel: NameHash(sandbox.Name)}},
				"spec": map[string]any{
					"runtimeClassName": "kata-pod-ckpt",
					"containers": []any{map[string]any{
						"name": "worker", "image": "registry.k8s.io/busybox:1.36.1", "command": []any{"sh", "-c", "sleep 3600"},
					}},
				},
			},
		},
	}}
	checkpoint.SetOwnerReferences([]metav1.OwnerReference{sandboxControllerRef(sandbox.Name)})
	checkpoint.SetLabels(map[string]string{sandboxv1beta1.SandboxNameHashLabel: NameHash(sandbox.Name)})
	checkpoint.SetUID(types.UID("checkpoint-uid"))
	return checkpoint
}

func checkpointTestReconciler(sandbox *sandboxv1beta1.Sandbox, pod *corev1.Pod, dynamicObjects ...runtime.Object) *SandboxReconciler {
	if sandbox.Status.CheckpointRestore != nil && !slices.Contains(sandbox.Finalizers, checkpointRestoreFinalizer) {
		sandbox.Finalizers = append(sandbox.Finalizers, checkpointRestoreFinalizer)
	}
	objects := []runtime.Object{sandbox}
	if pod != nil {
		objects = append(objects, pod)
	}
	return &SandboxReconciler{
		Client: newFakeClient(objects...),
		Scheme: Scheme,
		DynamicClient: dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
			map[schema.GroupVersionResource]string{podCheckpointGVR: "PodCheckpointList"}, dynamicObjects...),
		EnablePodCheckpointRestore:  true,
		PodCheckpointRuntimeClasses: map[string]struct{}{"kata-pod-ckpt": {}},
	}
}

func TestCheckpointRestoreCreatesUIDPinnedCheckpoint(t *testing.T) {
	ctx := context.Background()
	sandbox := checkpointTestSandbox(sandboxv1beta1.SandboxOperatingModeHibernated)
	pod := checkpointTestPod(sandbox)
	r := checkpointTestReconciler(sandbox, pod)

	handled, resultPod, err := r.reconcilePodCheckpointRestore(ctx, sandbox, pod)
	require.NoError(t, err)
	require.True(t, handled)
	require.Same(t, pod, resultPod)
	require.Nil(t, sandbox.Status.CheckpointRestore)
	persisted := &sandboxv1beta1.Sandbox{}
	require.NoError(t, r.Get(ctx, types.NamespacedName{Namespace: sandbox.Namespace, Name: sandbox.Name}, persisted))
	require.Contains(t, persisted.Finalizers, checkpointRestoreFinalizer)
	_, err = r.DynamicClient.Resource(podCheckpointGVR).Namespace(sandbox.Namespace).Get(ctx, checkpointName(checkpointOperationID(sandbox, pod)), metav1.GetOptions{})
	require.True(t, apierrors.IsNotFound(err))

	handled, resultPod, err = r.reconcilePodCheckpointRestore(ctx, sandbox, pod)
	require.NoError(t, err)
	require.True(t, handled)
	require.Same(t, pod, resultPod)
	require.NotNil(t, sandbox.Status.CheckpointRestore)

	checkpoint, err := r.DynamicClient.Resource(podCheckpointGVR).Namespace(sandbox.Namespace).Get(ctx, sandbox.Status.CheckpointRestore.CheckpointName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, pod.Name, checkpoint.Object["spec"].(map[string]any)["sourcePod"].(map[string]any)["name"])
	require.Equal(t, string(pod.UID), checkpoint.Object["spec"].(map[string]any)["sourcePod"].(map[string]any)["uid"])
}

func TestCheckpointRestoreReadsFullSourcePodSpec(t *testing.T) {
	ctx := context.Background()
	sandbox := checkpointTestSandbox(sandboxv1beta1.SandboxOperatingModeHibernated)
	source := checkpointTestPod(sandbox)
	r := checkpointTestReconciler(sandbox, source)
	r.APIReader = r.Client
	cached := source.DeepCopy()
	cached.Spec = corev1.PodSpec{NodeName: source.Spec.NodeName}

	handled, _, err := r.reconcilePodCheckpointRestore(ctx, sandbox, cached)
	require.NoError(t, err)
	require.True(t, handled)
	require.Nil(t, sandbox.Status.CheckpointRestore)
	handled, _, err = r.reconcilePodCheckpointRestore(ctx, sandbox, cached)
	require.NoError(t, err)
	require.True(t, handled)
	require.Equal(t, source.UID, sandbox.Status.CheckpointRestore.SourcePodUID)
}

func TestCheckpointRestoreDoesNotFinalizeUnreadySource(t *testing.T) {
	sandbox := checkpointTestSandbox(sandboxv1beta1.SandboxOperatingModeHibernated)
	pod := checkpointTestPod(sandbox)
	pod.Status.Conditions[0].Status = corev1.ConditionFalse
	r := checkpointTestReconciler(sandbox, pod)

	handled, _, err := r.reconcilePodCheckpointRestore(t.Context(), sandbox, pod)
	require.NoError(t, err)
	require.True(t, handled)
	require.NotContains(t, sandbox.Finalizers, checkpointRestoreFinalizer)
	require.Nil(t, sandbox.Status.CheckpointRestore)
}

func TestCheckpointRestoreFinalizerOnlyRunningConverges(t *testing.T) {
	for _, checkpointExists := range []bool{false, true} {
		t.Run(map[bool]string{false: "without checkpoint", true: "with unused checkpoint"}[checkpointExists], func(t *testing.T) {
			sandbox := checkpointTestSandbox(sandboxv1beta1.SandboxOperatingModeRunning)
			pod := checkpointTestPod(sandbox)
			sandbox.Finalizers = []string{checkpointRestoreFinalizer}
			var objects []runtime.Object
			if checkpointExists {
				operationID := checkpointOperationID(sandbox, pod)
				sandbox.Status.CheckpointRestore = &sandboxv1beta1.SandboxCheckpointRestoreStatus{
					CheckpointName: checkpointName(operationID), SourcePodName: pod.Name, SourcePodUID: pod.UID,
					SourceNodeName: pod.Spec.NodeName, OperationID: operationID,
				}
				objects = append(objects, readyCheckpoint(sandbox, checkpointName(operationID)))
				sandbox.Status.CheckpointRestore = nil
			}
			r := checkpointTestReconciler(sandbox, pod, objects...)

			handled, _, err := r.reconcilePodCheckpointRestore(t.Context(), sandbox, pod)
			require.NoError(t, err)
			require.True(t, handled)
			if checkpointExists {
				handled, _, err = r.reconcilePodCheckpointRestore(t.Context(), sandbox, pod)
				require.NoError(t, err)
				require.True(t, handled)
			}
			updated := &sandboxv1beta1.Sandbox{}
			require.NoError(t, r.Get(t.Context(), types.NamespacedName{Namespace: sandbox.Namespace, Name: sandbox.Name}, updated))
			require.NotContains(t, updated.Finalizers, checkpointRestoreFinalizer)
		})
	}
}

func TestCheckpointRestoreDeletesSourceOnlyAfterReady(t *testing.T) {
	ctx := context.Background()
	sandbox := checkpointTestSandbox(sandboxv1beta1.SandboxOperatingModeHibernated)
	pod := checkpointTestPod(sandbox)
	operationID := checkpointOperationID(sandbox, pod)
	sandbox.Status.CheckpointRestore = &sandboxv1beta1.SandboxCheckpointRestoreStatus{
		CheckpointName: checkpointName(operationID), SourcePodName: pod.Name, SourcePodUID: pod.UID, SourceNodeName: pod.Spec.NodeName, OperationID: operationID,
	}
	checkpoint := readyCheckpoint(sandbox, sandbox.Status.CheckpointRestore.CheckpointName)
	r := checkpointTestReconciler(sandbox, pod, checkpoint)

	sandbox.Status.CheckpointRestore.CheckpointUID = checkpoint.GetUID()
	handled, _, err := r.reconcilePodCheckpointRestore(ctx, sandbox, pod)
	require.NoError(t, err)
	require.True(t, handled)
	got := &corev1.Pod{}
	err = r.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}, got)
	require.True(t, apierrors.IsNotFound(err))
}

func TestCheckpointRestoreCreatesRestorePodFromCheckpointTemplate(t *testing.T) {
	ctx := context.Background()
	sandbox := checkpointTestSandbox(sandboxv1beta1.SandboxOperatingModeRunning)
	sandbox.Status.CheckpointRestore = &sandboxv1beta1.SandboxCheckpointRestoreStatus{
		CheckpointName: "as-ckpt-test", SourcePodName: sandbox.Name, SourcePodUID: types.UID("source-pod-uid"), SourceNodeName: "alex-master001", OperationID: "test",
	}
	checkpoint := readyCheckpoint(sandbox, sandbox.Status.CheckpointRestore.CheckpointName)
	r := checkpointTestReconciler(sandbox, nil, checkpoint)

	_, _, err := r.reconcilePodCheckpointRestore(ctx, sandbox, nil)
	require.NoError(t, err)
	require.Equal(t, checkpoint.GetUID(), sandbox.Status.CheckpointRestore.CheckpointUID)
	require.Equal(t, sandbox.Generation, sandbox.Status.CheckpointRestore.RestoreGeneration)
	handled, resultPod, err := r.reconcilePodCheckpointRestore(ctx, sandbox, nil)
	require.NoError(t, err)
	require.True(t, handled)
	require.Nil(t, resultPod)
	restored, err := r.DynamicClient.Resource(podGVR).Namespace(sandbox.Namespace).Get(ctx, sandbox.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, sandbox.Status.CheckpointRestore.CheckpointName, restored.Object["spec"].(map[string]any)["restoreFrom"].(map[string]any)["name"])
	_, hasNodeName := restored.Object["spec"].(map[string]any)["nodeName"]
	require.False(t, hasNodeName)
}

func TestCheckpointRestoreRejectsColdFallbackWhenDisabled(t *testing.T) {
	sandbox := checkpointTestSandbox(sandboxv1beta1.SandboxOperatingModeHibernated)
	r := checkpointTestReconciler(sandbox, nil)
	r.EnablePodCheckpointRestore = false

	handled, _, err := r.reconcilePodCheckpointRestore(context.Background(), sandbox, nil)
	require.NoError(t, err)
	require.True(t, handled)
	condition := meta.FindStatusCondition(sandbox.Status.Conditions, string(sandboxv1beta1.SandboxConditionCheckpointRestoreSupported))
	require.NotNil(t, condition)
	require.Equal(t, metav1.ConditionFalse, condition.Status)
	require.Equal(t, "Unsupported", condition.Reason)
	require.Contains(t, condition.Message, "disabled")
}

func TestCheckpointRestoreUnsupportedTracksCurrentGeneration(t *testing.T) {
	sandbox := checkpointTestSandbox(sandboxv1beta1.SandboxOperatingModeHibernated)
	setCheckpointCondition(sandbox, sandboxv1beta1.SandboxConditionCheckpointRestoreSupported, metav1.ConditionFalse, "Unsupported", "disabled")
	require.True(t, checkpointRestoreUnsupported(sandbox))
	sandbox.Generation++
	require.False(t, checkpointRestoreUnsupported(sandbox))
}

func TestUnsupportedManualHibernationDoesNotRequeue(t *testing.T) {
	sandbox := checkpointTestSandbox(sandboxv1beta1.SandboxOperatingModeHibernated)
	pod := checkpointTestPod(sandbox)
	r := checkpointTestReconciler(sandbox, pod)
	r.EnablePodCheckpointRestore = false
	r.APIReader = r.Client
	r.Tracer = asmetrics.NewNoOp()

	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: sandbox.Namespace, Name: sandbox.Name}}
	_, err := r.Reconcile(t.Context(), request)
	require.NoError(t, err)
	result, err := r.Reconcile(t.Context(), request)
	require.NoError(t, err)
	require.Zero(t, result.RequeueAfter)
	updated := &sandboxv1beta1.Sandbox{}
	require.NoError(t, r.Get(t.Context(), types.NamespacedName{Namespace: sandbox.Namespace, Name: sandbox.Name}, updated))
	require.True(t, checkpointRestoreUnsupported(updated), "conditions=%+v generation=%d", updated.Status.Conditions, updated.Generation)
	require.NotContains(t, updated.Finalizers, checkpointRestoreFinalizer)
}

func TestCheckpointRestoreReleasesFinalizerWhenStatusIsMissing(t *testing.T) {
	sandbox := checkpointTestSandbox(sandboxv1beta1.SandboxOperatingModeRunning)
	sandbox.Finalizers = []string{checkpointRestoreFinalizer}
	r := checkpointTestReconciler(sandbox, nil)

	handled, _, err := r.reconcilePodCheckpointRestore(context.Background(), sandbox, nil)
	require.NoError(t, err)
	require.True(t, handled)
	updated := &sandboxv1beta1.Sandbox{}
	require.NoError(t, r.Get(t.Context(), types.NamespacedName{Namespace: sandbox.Namespace, Name: sandbox.Name}, updated))
	require.NotContains(t, updated.Finalizers, checkpointRestoreFinalizer)
}

func TestCheckpointRestorePreservesCheckpointWhileSourceTerminates(t *testing.T) {
	ctx := context.Background()
	sandbox := checkpointTestSandbox(sandboxv1beta1.SandboxOperatingModeRunning)
	pod := checkpointTestPod(sandbox)
	now := metav1.Now()
	pod.DeletionTimestamp = &now
	pod.Finalizers = []string{"test.example/hold"}
	sandbox.Status.CheckpointRestore = &sandboxv1beta1.SandboxCheckpointRestoreStatus{
		CheckpointName: "test-checkpoint", SourcePodName: pod.Name, SourcePodUID: pod.UID,
	}
	checkpoint := readyCheckpoint(sandbox, "test-checkpoint")
	r := checkpointTestReconciler(sandbox, pod, checkpoint)

	handled, _, err := r.reconcilePodCheckpointRestore(ctx, sandbox, pod)
	require.NoError(t, err)
	require.True(t, handled)
	_, err = r.getOwnedPodCheckpoint(ctx, sandbox)
	require.NoError(t, err)
}

func TestCheckpointRestoreRejectsForeignCheckpoint(t *testing.T) {
	sandbox := checkpointTestSandbox(sandboxv1beta1.SandboxOperatingModeRunning)
	sandbox.Status.CheckpointRestore = &sandboxv1beta1.SandboxCheckpointRestoreStatus{
		CheckpointName: "test-checkpoint", SourcePodName: sandbox.Name, SourcePodUID: "source-uid",
	}
	checkpoint := readyCheckpoint(sandbox, "test-checkpoint")
	checkpoint.SetOwnerReferences(nil)
	r := checkpointTestReconciler(sandbox, nil, checkpoint)

	handled, _, err := r.reconcilePodCheckpointRestore(context.Background(), sandbox, nil)
	require.True(t, handled)
	require.ErrorContains(t, err, "conflicts")
}

func TestCheckpointRestoreCleanupWaitsForOwnedCheckpoint(t *testing.T) {
	ctx := context.Background()
	sandbox := checkpointTestSandbox(sandboxv1beta1.SandboxOperatingModeRunning)
	sandbox.Status.CheckpointRestore = &sandboxv1beta1.SandboxCheckpointRestoreStatus{
		CheckpointName: "owned", SourcePodName: sandbox.Name, SourcePodUID: "source-uid",
	}
	owned := readyCheckpoint(sandbox, "owned")
	foreign := readyCheckpoint(sandbox, "foreign")
	foreign.SetOwnerReferences(nil)
	r := checkpointTestReconciler(sandbox, nil, owned, foreign)

	pending, err := r.deleteOwnedPodCheckpoints(ctx, sandbox)
	require.NoError(t, err)
	require.True(t, pending)
	pending, err = r.deleteOwnedPodCheckpoints(ctx, sandbox)
	require.NoError(t, err)
	require.False(t, pending)
	_, err = r.DynamicClient.Resource(podCheckpointGVR).Namespace(sandbox.Namespace).Get(ctx, "foreign", metav1.GetOptions{})
	require.NoError(t, err)
}

func TestCheckpointRestoreRecoversUniqueBinding(t *testing.T) {
	ctx := context.Background()
	sandbox := checkpointTestSandbox(sandboxv1beta1.SandboxOperatingModeRunning)
	sandbox.Finalizers = []string{checkpointRestoreFinalizer}
	source := checkpointTestPod(sandbox)
	operation := checkpointOperationID(sandbox, source)
	sandbox.Status.CheckpointRestore = &sandboxv1beta1.SandboxCheckpointRestoreStatus{
		CheckpointName: checkpointName(operation), SourcePodName: source.Name, SourcePodUID: source.UID,
	}
	checkpoint := readyCheckpoint(sandbox, checkpointName(operation))
	sandbox.Status.CheckpointRestore = nil
	r := checkpointTestReconciler(sandbox, nil, checkpoint)

	handled, _, err := r.reconcilePodCheckpointRestore(ctx, sandbox, nil)
	require.NoError(t, err)
	require.True(t, handled)
	require.Equal(t, checkpoint.GetUID(), sandbox.Status.CheckpointRestore.CheckpointUID)
	require.Equal(t, source.UID, sandbox.Status.CheckpointRestore.SourcePodUID)
}

func TestCheckpointRestoreRejectsReplacementAndDrift(t *testing.T) {
	ctx := context.Background()
	sandbox := checkpointTestSandbox(sandboxv1beta1.SandboxOperatingModeRunning)
	sandbox.Status.CheckpointRestore = &sandboxv1beta1.SandboxCheckpointRestoreStatus{
		CheckpointName: "test-checkpoint", SourcePodName: sandbox.Name, SourcePodUID: "source-uid",
	}
	checkpoint := readyCheckpoint(sandbox, "test-checkpoint")
	hash, err := sandboxCheckpointTemplateHash(sandbox)
	require.NoError(t, err)
	checkpoint.SetAnnotations(map[string]string{checkpointTemplateHash: hash})
	r := checkpointTestReconciler(sandbox, nil, checkpoint)

	sandbox.Status.CheckpointRestore.CheckpointUID = "old-checkpoint-uid"
	_, err = r.getOwnedPodCheckpoint(ctx, sandbox)
	require.ErrorContains(t, err, "was replaced")
	sandbox.Status.CheckpointRestore.CheckpointUID = checkpoint.GetUID()
	sandbox.Spec.PodTemplate.Spec.Containers[0].Image = "different-image"
	_, err = r.getOwnedPodCheckpoint(ctx, sandbox)
	require.ErrorContains(t, err, "PodTemplate changed")
}
