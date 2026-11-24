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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
	asmetrics "sigs.k8s.io/agent-sandbox/internal/metrics"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestCheckpointUsesOwnerMappingInsteadOfForeignTrackingLabels(t *testing.T) {
	for _, owned := range []bool{false, true} {
		t.Run(map[bool]string{false: "foreign candidate", true: "multiple owned Pods"}[owned], func(t *testing.T) {
			ctx := context.Background()
			sandbox := checkpointTestSandbox(sandboxv1beta1.SandboxOperatingModeHibernated)
			source := checkpointTestPod(sandbox)
			extra := source.DeepCopy()
			extra.Name = "another-pod"
			extra.UID = "another-pod-uid"
			extra.Labels = map[string]string{sandboxv1beta1.SandboxNameHashLabel: NameHash(sandbox.Name)}
			if !owned {
				extra.OwnerReferences[0].UID = "foreign-sandbox-uid"
			}
			r := checkpointTestReconciler(sandbox, source)
			r.Tracer = asmetrics.NewNoOp()
			require.NoError(t, r.Create(ctx, extra))
			_, _, err := r.reconcilePod(ctx, sandbox, NameHash(sandbox.Name), nil)
			if owned {
				require.True(t, isMultipleSandboxPodsError(err), "ambiguous owned Pods must not start checkpoint effects")
				require.Nil(t, sandbox.Status.CheckpointRestore)
			} else {
				require.NoError(t, err)
				_, _, err = r.reconcilePod(ctx, sandbox, NameHash(sandbox.Name), nil)
				require.NoError(t, err)
				require.NotNil(t, sandbox.Status.CheckpointRestore)
				require.Equal(t, source.UID, sandbox.Status.CheckpointRestore.SourcePodUID)
			}
			remaining := &corev1.Pod{}
			require.NoError(t, r.Get(ctx, client.ObjectKeyFromObject(source), remaining))
			require.True(t, remaining.DeletionTimestamp.IsZero())
		})
	}
}

func TestCheckpointDeletionPreservesLifecycleBookkeepingCleanup(t *testing.T) {
	ctx := context.Background()
	sandbox := checkpointTestSandbox(sandboxv1beta1.SandboxOperatingModeRunning)
	sandbox.Finalizers = []string{checkpointRestoreFinalizer}
	sandbox.DeletionTimestamp = &metav1.Time{Time: metav1.Now().Time}
	r := checkpointTestReconciler(sandbox, nil)
	r.Tracer = asmetrics.NewNoOp()
	key := client.ObjectKeyFromObject(sandbox)
	r.suspendStartTimes.anchor(key, sandbox.UID, time.Now())
	r.resumeStartTimes.anchor(key, sandbox.UID, time.Now())
	r.recreateBackoff.recordFailure(key, "old-pod")
	// Finalizer work must retain main's lifecycle-state cleanup and never
	// enter the normal replacement path.
	_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
	require.NoError(t, err)
	require.False(t, r.recreateBackoff.has(key))
	_, present := r.suspendStartTimes.take(key, sandbox.UID)
	require.False(t, present)
	_, present = r.resumeStartTimes.take(key, sandbox.UID)
	require.False(t, present)
	var pods corev1.PodList
	require.NoError(t, r.List(ctx, &pods, client.InNamespace(sandbox.Namespace)))
	require.Empty(t, pods.Items)
}
