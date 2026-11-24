/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	asmetrics "sigs.k8s.io/agent-sandbox/internal/metrics"
)

func TestAutoSuspensionCompletionDoesNotStarveChildReconciliation(t *testing.T) {
	for _, existingPod := range []bool{false, true} {
		name := "creation"
		if existingPod {
			name = "ready-generation-update"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			sandbox := autoTestSandbox(autoGateOpen, time.Now())
			meta.SetStatusCondition(&sandbox.Status.Conditions, metav1.Condition{
				Type: string(sandboxv1beta1.SandboxConditionReady), Status: metav1.ConditionFalse,
				Reason: "Provisioning", ObservedGeneration: sandbox.Generation - 1,
			})
			var pod *corev1.Pod
			if existingPod {
				pod = checkpointTestPod(sandbox)
				pod.Status.PodIP = "10.0.0.23"
				pod.Status.PodIPs = []corev1.PodIP{{IP: "10.0.0.23"}}
			}
			r := checkpointTestReconciler(sandbox, pod)
			r.APIReader, r.Tracer = r.Client, asmetrics.NewNoOp()
			key := client.ObjectKeyFromObject(sandbox)
			for range 6 {
				require.NoError(t, r.Create(ctx, autoTestLease(sandbox, "Request", "Released")))
				_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
				require.NoError(t, err)
				require.NoError(t, r.Get(ctx, key, &corev1.Pod{}), "completion bookkeeping must not starve Pod creation")
				require.NoError(t, r.Get(ctx, key, sandbox))
				if existingPod {
					ready := meta.FindStatusCondition(sandbox.Status.Conditions, string(sandboxv1beta1.SandboxConditionReady))
					require.NotNil(t, ready)
					require.Equal(t, metav1.ConditionTrue, ready.Status)
					require.Equal(t, sandbox.Generation, ready.ObservedGeneration)
				}
			}
		})
	}
}

func autoTestSandbox(gate string, now time.Time) *sandboxv1beta1.Sandbox {
	sandbox := checkpointTestSandbox(sandboxv1beta1.SandboxOperatingModeRunning)
	sandbox.Spec.AutoSuspension = &sandboxv1beta1.SandboxAutoSuspensionSpec{IdleTimeoutSeconds: 10, MaxCycles: 2, ActivityMode: "RoutedRequestsAndLeases"}
	last := metav1.NewTime(now.Add(-time.Minute))
	sandbox.Status.AutoSuspension = &sandboxv1beta1.SandboxAutoSuspensionStatus{Gate: gate, Epoch: 7, LastActivityTime: &last}
	meta.SetStatusCondition(&sandbox.Status.Conditions, metav1.Condition{Type: string(sandboxv1beta1.SandboxConditionReady), Status: metav1.ConditionTrue, Reason: "Ready"})
	return sandbox
}

func autoTestLease(sandbox *sandboxv1beta1.Sandbox, kind, state string) *coordinationv1.Lease {
	ancient := metav1.NewMicroTime(time.Unix(1, 0))
	return &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: "activity", Namespace: sandbox.Namespace, Labels: map[string]string{activitySandboxUIDLabel: string(sandbox.UID), activityKindLabel: kind, activityStateLabel: state}, OwnerReferences: []metav1.OwnerReference{sandboxControllerRef(sandbox.Name)}}, Spec: coordinationv1.LeaseSpec{RenewTime: &ancient}}
}

func TestAutoSuspensionDrainingReservesBudgetOnceAndClearsPreviousConditions(t *testing.T) {
	ctx, now := context.Background(), time.Now()
	sandbox := autoTestSandbox(autoGateDraining, now)
	meta.SetStatusCondition(&sandbox.Status.Conditions, metav1.Condition{Type: string(sandboxv1beta1.SandboxConditionHibernated), Status: metav1.ConditionTrue, Reason: "PreviousCycle"})
	r := checkpointTestReconciler(sandbox, checkpointTestPod(sandbox))
	barrier, err := r.reconcileAutoSuspension(ctx, sandbox, now)
	require.NoError(t, err)
	require.True(t, barrier)
	require.Equal(t, autoGateCheckpointing, sandbox.Status.AutoSuspension.Gate)
	require.EqualValues(t, 1, sandbox.Status.AutoSuspension.Cycles)
	hibernated := meta.FindStatusCondition(sandbox.Status.Conditions, string(sandboxv1beta1.SandboxConditionHibernated))
	require.NotNil(t, hibernated)
	require.Equal(t, metav1.ConditionFalse, hibernated.Status)
	require.Equal(t, "NewCheckpointCycle", hibernated.Reason)
	require.NoError(t, r.Get(ctx, client.ObjectKeyFromObject(sandbox), sandbox))
	barrier, err = r.reconcileAutoSuspension(ctx, sandbox, now.Add(time.Second))
	require.NoError(t, err)
	require.False(t, barrier)
	require.EqualValues(t, 1, sandbox.Status.AutoSuspension.Cycles)
	require.Equal(t, sandboxv1beta1.SandboxOperatingModeRunning, sandbox.Spec.OperatingMode)
	require.Equal(t, sandboxv1beta1.SandboxOperatingModeHibernated, effectiveOperatingMode(sandbox))
}

func TestAutoSuspensionReleasedDuringDrainRestartsQuietInterval(t *testing.T) {
	ctx, now := context.Background(), time.Now()
	sandbox := autoTestSandbox(autoGateDraining, now)
	lease := autoTestLease(sandbox, "Request", "Released")
	r := checkpointTestReconciler(sandbox, checkpointTestPod(sandbox))
	require.NoError(t, r.Create(ctx, lease))
	barrier, err := r.reconcileAutoSuspension(ctx, sandbox, now)
	require.NoError(t, err)
	require.True(t, barrier)
	require.Equal(t, autoGateOpen, sandbox.Status.AutoSuspension.Gate)
	require.Equal(t, now.Unix(), sandbox.Status.AutoSuspension.LastActivityTime.Unix())
	require.EqualValues(t, 0, sandbox.Status.AutoSuspension.Cycles)
	require.True(t, apierrors.IsNotFound(r.Get(ctx, client.ObjectKeyFromObject(lease), &coordinationv1.Lease{})))
	got := &sandboxv1beta1.Sandbox{}
	require.NoError(t, r.Get(ctx, client.ObjectKeyFromObject(sandbox), got))
	require.Equal(t, now.Truncate(time.Second).UTC(), got.Status.AutoSuspension.LastActivityTime.Time.UTC())
}

func TestAutoSuspensionNeverExpiresActiveBackgroundOrRequest(t *testing.T) {
	for _, kind := range []string{"Request", "Background"} {
		t.Run(kind, func(t *testing.T) {
			ctx, now := context.Background(), time.Now()
			sandbox := autoTestSandbox(autoGateOpen, now)
			lease := autoTestLease(sandbox, kind, "Active")
			r := checkpointTestReconciler(sandbox, checkpointTestPod(sandbox))
			require.NoError(t, r.Create(ctx, lease))
			barrier, err := r.reconcileAutoSuspension(ctx, sandbox, now)
			require.NoError(t, err)
			require.False(t, barrier)
			require.Equal(t, autoGateOpen, sandbox.Status.AutoSuspension.Gate)
			require.NoError(t, r.Get(ctx, client.ObjectKeyFromObject(lease), &coordinationv1.Lease{}))
		})
	}
}

func TestAutoSuspensionBudgetAndMissingSourceBlockNewCheckpoint(t *testing.T) {
	ctx, now := context.Background(), time.Now()
	sandbox := autoTestSandbox(autoGateOpen, now)
	sandbox.Status.AutoSuspension.Cycles = 2
	r := checkpointTestReconciler(sandbox, checkpointTestPod(sandbox))
	barrier, err := r.reconcileAutoSuspension(ctx, sandbox, now)
	require.NoError(t, err)
	require.False(t, barrier)
	require.Equal(t, autoGateOpen, sandbox.Status.AutoSuspension.Gate)
	sandbox = autoTestSandbox(autoGateOpen, now)
	r = checkpointTestReconciler(sandbox, nil)
	barrier, err = r.reconcileAutoSuspension(ctx, sandbox, now)
	require.NoError(t, err)
	require.False(t, barrier)
	require.Equal(t, autoGateOpen, sandbox.Status.AutoSuspension.Gate)
}

func TestAutoSuspensionManualModeCannotRestoreOrReopen(t *testing.T) {
	ctx, now := context.Background(), time.Now()
	sandbox := autoTestSandbox(autoGateHibernated, now)
	sandbox.Spec.OperatingMode = sandboxv1beta1.SandboxOperatingModeSuspended
	r := checkpointTestReconciler(sandbox, nil)
	barrier, err := r.reconcileAutoSuspension(ctx, sandbox, now)
	require.NoError(t, err)
	require.False(t, barrier)
	require.Equal(t, autoGateHibernated, sandbox.Status.AutoSuspension.Gate)
	require.Equal(t, sandboxv1beta1.SandboxOperatingModeSuspended, effectiveOperatingMode(sandbox))
}

func TestAutoSuspensionMissingGateFailsClosedWithCheckpointBinding(t *testing.T) {
	ctx, now := context.Background(), time.Now()
	sandbox := autoTestSandbox(autoGateOpen, now)
	sandbox.Status.AutoSuspension = nil
	sandbox.Finalizers = []string{checkpointRestoreFinalizer}
	r := checkpointTestReconciler(sandbox, checkpointTestPod(sandbox))
	_, err := r.reconcileAutoSuspension(ctx, sandbox, now)
	require.ErrorContains(t, err, "admission state is missing")
	require.Nil(t, sandbox.Status.AutoSuspension)
}

func TestCheckpointDeleteFencedByAutomaticEpochAndOperation(t *testing.T) {
	now := time.Now()
	expected := autoTestSandbox(autoGateCheckpointing, now)
	expected.Status.CheckpointRestore = &sandboxv1beta1.SandboxCheckpointRestoreStatus{OperationID: "cycle-a", SourcePodUID: "source-a", CheckpointUID: "checkpoint-a"}
	current := expected.DeepCopy()
	require.True(t, checkpointIntentMatches(expected, current))
	current.Status.AutoSuspension.Epoch++
	require.False(t, checkpointIntentMatches(expected, current))
	current = expected.DeepCopy()
	current.Status.CheckpointRestore.OperationID = "cycle-b"
	require.False(t, checkpointIntentMatches(expected, current))
	current = expected.DeepCopy()
	current.Spec.OperatingMode = sandboxv1beta1.SandboxOperatingModeSuspended
	require.False(t, checkpointIntentMatches(expected, current))
}

type autoConflictWriter struct{ client.SubResourceWriter }

func (w autoConflictWriter) Update(_ context.Context, obj client.Object, _ ...client.SubResourceUpdateOption) error {
	return apierrors.NewConflict(schema.GroupResource{Group: "agents.x-k8s.io", Resource: "sandboxes"}, obj.GetName(), errors.New("injected conflict"))
}

type autoConflictClient struct{ client.Client }

func (c autoConflictClient) Status() client.SubResourceWriter {
	return autoConflictWriter{c.Client.Status()}
}

func TestAutoSuspensionConflictPreservesCompletionEvidence(t *testing.T) {
	ctx, now := context.Background(), time.Now()
	sandbox := autoTestSandbox(autoGateDraining, now)
	lease := autoTestLease(sandbox, "Request", "Released")
	r := checkpointTestReconciler(sandbox, checkpointTestPod(sandbox))
	require.NoError(t, r.Create(ctx, lease))
	original := r.Client
	r.Client = autoConflictClient{original}
	r.APIReader = original
	_, err := r.reconcileAutoSuspension(ctx, sandbox, now)
	require.True(t, apierrors.IsConflict(err))
	require.NoError(t, original.Get(ctx, client.ObjectKeyFromObject(lease), &coordinationv1.Lease{}))
	got := &sandboxv1beta1.Sandbox{}
	require.NoError(t, original.Get(ctx, client.ObjectKeyFromObject(sandbox), got))
	require.Equal(t, autoGateDraining, got.Status.AutoSuspension.Gate)
	require.EqualValues(t, 0, got.Status.AutoSuspension.Cycles)
}

func TestAutoSuspensionManualCheckpointClosesBeforeEffectAndRunningMustFinish(t *testing.T) {
	ctx, now := context.Background(), time.Now()
	sandbox := autoTestSandbox(autoGateOpen, now)
	sandbox.Spec.OperatingMode = sandboxv1beta1.SandboxOperatingModeHibernated
	r := checkpointTestReconciler(sandbox, checkpointTestPod(sandbox))
	barrier, err := r.reconcileAutoSuspension(ctx, sandbox, now)
	require.NoError(t, err)
	require.True(t, barrier)
	require.Equal(t, autoGateCheckpointing, sandbox.Status.AutoSuspension.Gate)
	require.Equal(t, sandboxv1beta1.SandboxManualCheckpointRequested, sandbox.Status.AutoSuspension.ManualCheckpoint)
	require.EqualValues(t, 0, sandbox.Status.AutoSuspension.Cycles)
	// Reversal does not invoke the old early-cancel branch for an opt-in Pod.
	sandbox.Spec.OperatingMode = sandboxv1beta1.SandboxOperatingModeRunning
	require.Equal(t, sandboxv1beta1.SandboxOperatingModeHibernated, effectiveOperatingMode(sandbox))
	require.NoError(t, r.Update(ctx, sandbox))
	meta.SetStatusCondition(&sandbox.Status.Conditions, metav1.Condition{Type: string(sandboxv1beta1.SandboxConditionHibernated), Status: metav1.ConditionTrue, Reason: "SourcePodDeleted"})
	require.NoError(t, r.Status().Update(ctx, sandbox))
	barrier, err = r.reconcileAutoSuspension(ctx, sandbox, now)
	require.NoError(t, err)
	require.True(t, barrier)
	require.Equal(t, autoGateHibernated, sandbox.Status.AutoSuspension.Gate)
	barrier, err = r.reconcileAutoSuspension(ctx, sandbox, now)
	require.NoError(t, err)
	require.True(t, barrier)
	require.Equal(t, autoGateRestoring, sandbox.Status.AutoSuspension.Gate)
}

func TestRemovedAutoSuspensionPolicyStopsAfterReopening(t *testing.T) {
	sandbox := autoTestSandbox(autoGateOpen, time.Now())
	sandbox.Spec.AutoSuspension = nil
	require.False(t, hasAutoSuspension(sandbox))

	sandbox.Status.AutoSuspension.Gate = autoGateRestoring
	require.True(t, hasAutoSuspension(sandbox))
}

func TestUnsupportedAutoSuspensionDoesNotInitializeGate(t *testing.T) {
	sandbox := autoTestSandbox(autoGateOpen, time.Now())
	sandbox.Status.AutoSuspension = nil
	r := checkpointTestReconciler(sandbox, checkpointTestPod(sandbox))
	r.EnablePodCheckpointRestore = false

	barrier, err := r.reconcileAutoSuspension(context.Background(), sandbox, time.Now())
	require.NoError(t, err)
	require.False(t, barrier)
	require.Nil(t, sandbox.Status.AutoSuspension)
	require.True(t, checkpointRestoreUnsupported(sandbox))
}

func TestUnsupportedManualHibernationDoesNotEnterCheckpointing(t *testing.T) {
	sandbox := autoTestSandbox(autoGateOpen, time.Now())
	sandbox.Spec.OperatingMode = sandboxv1beta1.SandboxOperatingModeHibernated
	sandbox.Status.AutoSuspension = nil
	r := checkpointTestReconciler(sandbox, checkpointTestPod(sandbox))
	r.EnablePodCheckpointRestore = false

	barrier, err := r.reconcileAutoSuspension(t.Context(), sandbox, time.Now())
	require.NoError(t, err)
	require.False(t, barrier)
	require.Nil(t, sandbox.Status.AutoSuspension)
	require.True(t, checkpointRestoreUnsupported(sandbox))
}

func TestUnsupportedAutoSuspensionDoesNotRequeue(t *testing.T) {
	sandbox := autoTestSandbox(autoGateOpen, time.Now())
	sandbox.Status.AutoSuspension = nil
	pod := checkpointTestPod(sandbox)
	r := checkpointTestReconciler(sandbox, pod)
	r.EnablePodCheckpointRestore = false
	r.APIReader = r.Client
	r.Tracer = asmetrics.NewNoOp()

	key := client.ObjectKeyFromObject(sandbox)
	result, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: key})
	require.NoError(t, err)
	require.Zero(t, result.RequeueAfter)
	updated := &sandboxv1beta1.Sandbox{}
	require.NoError(t, r.Get(t.Context(), key, updated))
	require.True(t, checkpointRestoreUnsupported(updated))
}
