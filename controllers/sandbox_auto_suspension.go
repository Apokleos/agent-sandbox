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
	"fmt"
	"slices"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	sandboxv1beta1 "sigs.k8s.io/agent-sandbox/api/v1beta1"
)

const (
	autoGateOpen            = "Open"
	autoGateDraining        = "Draining"
	autoGateCheckpointing   = "Checkpointing"
	autoGateHibernated      = "Hibernated"
	autoGateRestoring       = "Restoring"
	activitySandboxUIDLabel = "agents.x-k8s.io/activity-sandbox-uid"
	activityKindLabel       = "agents.x-k8s.io/activity-kind"
	activityStateLabel      = "agents.x-k8s.io/activity-state"
	autoPollInterval        = 2 * time.Second
)

func hasAutoSuspension(sandbox *sandboxv1beta1.Sandbox) bool {
	if sandbox.Spec.AutoSuspension != nil {
		return true
	}
	state := sandbox.Status.AutoSuspension
	return state != nil && state.Gate != autoGateOpen
}

// effectiveOperatingMode never modifies user intent. A committed automatic
// checkpoint must finish before a pending Wake or policy removal can restore it.
func effectiveOperatingMode(sandbox *sandboxv1beta1.Sandbox) sandboxv1beta1.SandboxOperatingMode {
	mode := sandbox.Spec.OperatingMode
	if mode == "" {
		mode = sandboxv1beta1.SandboxOperatingModeRunning
	}
	if mode != sandboxv1beta1.SandboxOperatingModeRunning {
		return mode
	}
	if state := sandbox.Status.AutoSuspension; state != nil {
		switch state.Gate {
		case autoGateCheckpointing, autoGateHibernated:
			return sandboxv1beta1.SandboxOperatingModeHibernated
		}
	}
	return mode
}

func (r *SandboxReconciler) activityReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

type sandboxActivity struct {
	active   bool
	wake     bool
	released []coordinationv1.Lease
}

// List uses the direct API reader: a stale informer list would invalidate the
// create-ticket/recheck-gate ordering proof. Unknown owned tickets fail closed.
func (r *SandboxReconciler) readSandboxActivity(ctx context.Context, sandbox *sandboxv1beta1.Sandbox) (sandboxActivity, error) {
	var result sandboxActivity
	leases := &coordinationv1.LeaseList{}
	if err := r.activityReader().List(ctx, leases, client.InNamespace(sandbox.Namespace), client.MatchingLabels{activitySandboxUIDLabel: string(sandbox.UID)}); err != nil {
		return result, err
	}
	for _, lease := range leases.Items {
		owner := metav1.GetControllerOf(&lease)
		// Helpers may set a non-controller owner reference; exact UID is still
		// mandatory, and foreign resources never become our cleanup targets.
		owned := owner != nil && owner.UID == sandbox.UID && owner.Kind == "Sandbox"
		if !owned {
			for _, ref := range lease.OwnerReferences {
				if ref.UID == sandbox.UID && ref.Kind == "Sandbox" {
					owned = true
				}
			}
		}
		if !owned {
			return result, fmt.Errorf("activity Lease %q has no matching Sandbox owner", lease.Name)
		}
		kind, state := lease.Labels[activityKindLabel], lease.Labels[activityStateLabel]
		if kind != "Request" && kind != "Background" && kind != "Wake" {
			return result, fmt.Errorf("activity Lease %q has invalid kind", lease.Name)
		}
		switch state {
		case "Released":
			result.released = append(result.released, lease)
		case "Active":
			if kind == "Wake" {
				result.wake = true
			} else {
				result.active = true
			}
		default:
			return result, fmt.Errorf("activity Lease %q has invalid state", lease.Name)
		}
	}
	return result, nil
}

func setAutoCondition(sandbox *sandboxv1beta1.Sandbox, kind string, value metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(&sandbox.Status.Conditions, metav1.Condition{Type: kind, Status: value, Reason: reason, Message: message, ObservedGeneration: sandbox.Generation})
}

// reconcileAutoSuspension runs before child reconciliation. A true result means
// a status barrier was committed and this pass must not perform provider effects.
func (r *SandboxReconciler) reconcileAutoSuspension(ctx context.Context, sandbox *sandboxv1beta1.Sandbox, now time.Time) (bool, error) {
	if !hasAutoSuspension(sandbox) {
		return false, nil
	}
	policy, state := sandbox.Spec.AutoSuspension, sandbox.Status.AutoSuspension
	manual := sandbox.Spec.OperatingMode != "" && sandbox.Spec.OperatingMode != sandboxv1beta1.SandboxOperatingModeRunning
	if policy != nil {
		if policy.IdleTimeoutSeconds < 1 || policy.MaxCycles < 1 || policy.MaxCycles > 10 || policy.ActivityMode != "RoutedRequestsAndLeases" {
			return false, fmt.Errorf("invalid autoSuspension policy")
		}
		if !manual || sandbox.Spec.OperatingMode == sandboxv1beta1.SandboxOperatingModeHibernated {
			if err := r.validateCheckpointRestoreProfile(sandbox); err != nil {
				setCheckpointCondition(sandbox, sandboxv1beta1.SandboxConditionCheckpointRestoreSupported, metav1.ConditionFalse, "Unsupported", err.Error())
				return false, nil
			}
			setCheckpointCondition(sandbox, sandboxv1beta1.SandboxConditionCheckpointRestoreSupported, metav1.ConditionTrue, "Supported", "Pod-level checkpoint/restore is enabled for this Sandbox")
		}
	}
	if state == nil {
		if sandbox.Status.CheckpointRestore != nil || slices.Contains(sandbox.Finalizers, checkpointRestoreFinalizer) {
			return false, fmt.Errorf("automatic admission state is missing during an existing checkpoint operation; refusing to reopen")
		}
		timestamp := metav1.NewTime(now)
		sandbox.Status.AutoSuspension = &sandboxv1beta1.SandboxAutoSuspensionStatus{Gate: autoGateOpen, Epoch: 1, LastActivityTime: &timestamp}
		if sandbox.Spec.OperatingMode == sandboxv1beta1.SandboxOperatingModeHibernated {
			sandbox.Status.AutoSuspension.Gate = autoGateCheckpointing
			sandbox.Status.AutoSuspension.ManualCheckpoint = sandboxv1beta1.SandboxManualCheckpointRequested
		}
		return true, r.Status().Update(ctx, sandbox)
	}
	if manual {
		if sandbox.Spec.OperatingMode == sandboxv1beta1.SandboxOperatingModeHibernated && state.ManualCheckpoint == "" {
			state.ManualCheckpoint = sandboxv1beta1.SandboxManualCheckpointRequested
			state.Gate = autoGateCheckpointing
			state.Epoch++
			resetCheckpointConditions(sandbox, "NewCheckpointCycle", "A new checkpoint cycle was requested")
			return true, r.Status().Update(ctx, sandbox)
		}
		return false, nil // manual Suspended remains destructive and never restores
	}
	activity, err := r.readSandboxActivity(ctx, sandbox)
	if err != nil {
		return false, err
	}
	changed := false
	barrier := false
	if len(activity.released) != 0 {
		timestamp := metav1.NewTime(now)
		state.LastActivityTime = &timestamp // conservative server-side observation, never router clock
		changed = true
	}
	transition := func(gate string) {
		state.Gate = gate
		state.Epoch++
		changed = true
		barrier = true
	}
	reopen := func() {
		transition(autoGateOpen)
		timestamp := metav1.NewTime(now)
		state.LastActivityTime = &timestamp
		state.ManualCheckpoint = ""
		setAutoCondition(sandbox, "AutoSuspended", metav1.ConditionFalse, "Running", "Automatic hibernation is not active")
	}
	quiet := policy != nil && state.LastActivityTime != nil && !now.Before(state.LastActivityTime.Add(time.Duration(policy.IdleTimeoutSeconds)*time.Second))
	switch state.Gate {
	case autoGateOpen:
		// Startup is not idle time. Keep observing activity until Ready is
		// published; the next ready pass starts from this conservative clock.
		if !meta.IsStatusConditionTrue(sandbox.Status.Conditions, string(sandboxv1beta1.SandboxConditionReady)) {
			timestamp := metav1.NewTime(now)
			state.LastActivityTime = &timestamp
			quiet = false
		}
		setAutoCondition(sandbox, "ActivityBlocked", metav1.ConditionFalse, "NoActiveTickets", "No registered work is active")
		if activity.active {
			setAutoCondition(sandbox, "ActivityBlocked", metav1.ConditionTrue, "ActiveTickets", "Registered work blocks automatic hibernation; tickets never expire automatically")
		}
		// Running after a manual checkpoint must restore through a closed gate.
		if sandbox.Status.CheckpointRestore != nil && meta.IsStatusConditionTrue(sandbox.Status.Conditions, string(sandboxv1beta1.SandboxConditionHibernated)) && !meta.IsStatusConditionTrue(sandbox.Status.Conditions, string(sandboxv1beta1.SandboxConditionRestoreReady)) {
			transition(autoGateRestoring)
		} else if policy != nil && quiet && !activity.active && !activity.wake && state.Cycles < policy.MaxCycles && meta.IsStatusConditionTrue(sandbox.Status.Conditions, string(sandboxv1beta1.SandboxConditionReady)) {
			source := &corev1.Pod{}
			err := r.activityReader().Get(ctx, client.ObjectKey{Namespace: sandbox.Namespace, Name: resolvePodName(sandbox)}, source)
			if err != nil && !apierrors.IsNotFound(err) {
				return false, err
			}
			if err == nil && isOwnedBySandbox(source, sandbox) && podReady(source) && source.DeletionTimestamp.IsZero() {
				transition(autoGateDraining)
			} else {
				timestamp := metav1.NewTime(now)
				state.LastActivityTime = &timestamp
			}
		}
	case autoGateDraining:
		// A completion observed while closing starts a fresh quiet interval.
		if policy == nil || activity.active || activity.wake || !quiet || state.Cycles >= policy.MaxCycles {
			reopen()
		} else {
			// Reserve exactly once, in the same durable update as Checkpointing.
			state.Cycles++
			state.ManualCheckpoint = ""
			transition(autoGateCheckpointing)
			// Conditions describe the previous completed cycle until reset.
			// Clear them in the reservation barrier, before the provider runs.
			resetCheckpointConditions(sandbox, "NewCheckpointCycle", "A new automatic checkpoint cycle was reserved")
		}
	case autoGateCheckpointing:
		if meta.IsStatusConditionTrue(sandbox.Status.Conditions, string(sandboxv1beta1.SandboxConditionHibernated)) {
			transition(autoGateHibernated)
			setAutoCondition(sandbox, "AutoSuspended", metav1.ConditionTrue, "CheckpointRetained", "Source Pod is absent and its checkpoint is retained")
		}
	case autoGateHibernated:
		if policy == nil || activity.wake || state.ManualCheckpoint == sandboxv1beta1.SandboxManualCheckpointRequested {
			transition(autoGateRestoring)
		}
	case autoGateRestoring:
		if meta.IsStatusConditionTrue(sandbox.Status.Conditions, string(sandboxv1beta1.SandboxConditionRestoreReady)) {
			// The condition alone may be stale; bind opening to the exact
			// restored, owned, Ready Pod observed directly from the API.
			cr := sandbox.Status.CheckpointRestore
			if cr == nil || cr.RestoredPodUID == "" {
				return false, fmt.Errorf("restore readiness lacks a bound restored Pod")
			}
			pod := &corev1.Pod{}
			if err := r.activityReader().Get(ctx, client.ObjectKey{Namespace: sandbox.Namespace, Name: resolvePodName(sandbox)}, pod); err != nil {
				return false, err
			}
			if pod.UID == cr.RestoredPodUID && isOwnedBySandbox(pod, sandbox) && podReady(pod) && pod.DeletionTimestamp.IsZero() {
				reopen()
			}
		}
	default:
		return false, fmt.Errorf("unknown automatic admission gate %q", state.Gate)
	}
	if !changed {
		return false, nil
	}
	// Optimistic status Update is the barrier. Never perform checkpoint work
	// after a conflict, nor delete completion evidence before this succeeds.
	if err := r.Status().Update(ctx, sandbox); err != nil {
		return true, err
	}
	for i := range activity.released {
		lease := &activity.released[i]
		uid, version := lease.UID, lease.ResourceVersion
		if err := r.Delete(ctx, lease, client.Preconditions{UID: &uid, ResourceVersion: &version}); err != nil && !apierrors.IsNotFound(err) {
			return true, err
		}
	}
	// Completion accounting must not suppress Pod creation or Ready updates.
	// Only admission transitions require a new reconcile before child effects.
	return barrier, nil
}

// checkpointIntentMatches fences irreversible source deletion with both user
// generation and the allocated automatic operation. Status-only transitions do
// not increment metadata.generation.
func checkpointIntentMatches(expected, current *sandboxv1beta1.Sandbox) bool {
	if current.UID != expected.UID || current.Generation != expected.Generation || !current.DeletionTimestamp.IsZero() || effectiveOperatingMode(current) != sandboxv1beta1.SandboxOperatingModeHibernated {
		return false
	}
	a, b := expected.Status.AutoSuspension, current.Status.AutoSuspension
	if (a == nil) != (b == nil) {
		return false
	}
	if a != nil && (a.Epoch != b.Epoch || a.Gate != b.Gate || a.Cycles != b.Cycles) {
		return false
	}
	oldCycle, newCycle := expected.Status.CheckpointRestore, current.Status.CheckpointRestore
	return oldCycle != nil && newCycle != nil && oldCycle.OperationID == newCycle.OperationID && oldCycle.SourcePodUID == newCycle.SourcePodUID && oldCycle.CheckpointUID == newCycle.CheckpointUID
}
