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

package activity

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func fixture() (*dynamicfake.FakeDynamicClient, *kubefake.Clientset) {
	s := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "agents.x-k8s.io/v1beta1", "kind": "Sandbox",
		"metadata": map[string]any{"name": "work", "namespace": "test", "uid": "sandbox-uid", "generation": int64(1)},
		"spec":     map[string]any{"operatingMode": "Running", "autoSuspension": map[string]any{"idleTimeoutSeconds": int64(60)}},
		"status":   map[string]any{"autoSuspension": map[string]any{"gate": "Open", "epoch": int64(7)}, "conditions": []any{map[string]any{"type": "Ready", "status": "True", "observedGeneration": int64(1)}}},
	}}
	controller := true
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "work", Namespace: "test", UID: "pod-uid", OwnerReferences: []metav1.OwnerReference{{APIVersion: s.GetAPIVersion(), Kind: "Sandbox", Name: s.GetName(), UID: s.GetUID(), Controller: &controller}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0.23", Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
	d := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	if _, err := d.Resource(SandboxGVR).Namespace("test").Create(context.Background(), s, metav1.CreateOptions{}); err != nil {
		panic(err)
	}
	return d, kubefake.NewClientset(pod)
}

func TestOptedInStopsAfterPolicyRemovalAndReopen(t *testing.T) {
	sandbox := &unstructured.Unstructured{Object: map[string]any{
		"status": map[string]any{"autoSuspension": map[string]any{"gate": "Open"}},
	}}
	if optedIn(sandbox) {
		t.Fatal("an open Sandbox with no policy must not keep activity admission enabled")
	}
	if err := unstructured.SetNestedField(sandbox.Object, "Restoring", "status", "autoSuspension", "gate"); err != nil {
		t.Fatal(err)
	}
	if !optedIn(sandbox) {
		t.Fatal("an in-flight restore must keep activity admission enabled")
	}
}

func TestOptedInStopsForCurrentUnsupportedProfile(t *testing.T) {
	sandbox := &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"generation": int64(2)},
		"spec":     map[string]any{"autoSuspension": map[string]any{"idleTimeoutSeconds": int64(60)}},
		"status": map[string]any{"conditions": []any{map[string]any{
			"type": "CheckpointRestoreSupported", "status": "False", "reason": "Unsupported", "observedGeneration": int64(2),
		}}},
	}}
	if optedIn(sandbox) {
		t.Fatal("an unsupported automatic policy must not enable activity admission")
	}
	sandbox.SetGeneration(3)
	if !optedIn(sandbox) {
		t.Fatal("a stale unsupported condition must not suppress a changed policy")
	}
}

func TestUnsupportedProfileStopsAdmissionAlreadyInProgress(t *testing.T) {
	d, k := fixture()
	gets := 0
	d.PrependReactor("get", "sandboxes", func(_ ktesting.Action) (bool, runtime.Object, error) {
		gets++
		if gets != 2 {
			return false, nil, nil
		}
		obj, err := d.Tracker().Get(SandboxGVR, "test", "work")
		if err != nil {
			return true, nil, err
		}
		sandbox := obj.(*unstructured.Unstructured).DeepCopy()
		conditions := []any{map[string]any{
			"type": "CheckpointRestoreSupported", "status": "False", "reason": "Unsupported", "observedGeneration": sandbox.GetGeneration(),
		}}
		if err := unstructured.SetNestedSlice(sandbox.Object, conditions, "status", "conditions"); err != nil {
			return true, nil, err
		}
		if err := d.Tracker().Update(SandboxGVR, sandbox, "test"); err != nil {
			return true, nil, err
		}
		return true, sandbox, nil
	})

	permit, err := New(d, k, Options{PollInterval: time.Millisecond}).Acquire(context.Background(), "test", "work", Request)
	if err != nil || permit != nil {
		t.Fatalf("unsupported in-flight admission: permit=%+v err=%v", permit, err)
	}
	leases, err := k.CoordinationV1().Leases("test").List(context.Background(), metav1.ListOptions{})
	if err != nil || len(leases.Items) != 0 {
		t.Fatalf("tickets after unsupported result=%+v err=%v", leases.Items, err)
	}
}

func TestNotReadyWaitCreatesOnlyWakeDemand(t *testing.T) {
	d, k := fixture()
	s, err := d.Resource(SandboxGVR).Namespace("test").Get(context.Background(), "work", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	unstructured.RemoveNestedField(s.Object, "status", "conditions")
	if _, err = d.Resource(SandboxGVR).Namespace("test").Update(context.Background(), s, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	var kinds []string
	k.PrependReactor("create", "leases", func(a ktesting.Action) (bool, runtime.Object, error) {
		obj := a.(ktesting.CreateAction).GetObject().(metav1.Object)
		kinds = append(kinds, obj.GetLabels()[KindLabel])
		return false, nil, nil
	})
	c := New(d, k, Options{PollInterval: time.Millisecond, WaitTimeout: 30 * time.Millisecond})
	if permit, err := c.Acquire(context.Background(), "test", "work", Request); permit != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unready admission: permit=%v err=%v", permit, err)
	}
	if !reflect.DeepEqual(kinds, []string{Wake}) {
		t.Fatalf("unready wait must not churn ungranted Request tickets: %v", kinds)
	}
}

func TestPermitPinsReadyPodAndRetainsCompletion(t *testing.T) {
	for _, kind := range []string{Request, Background} {
		t.Run(kind, func(t *testing.T) {
			d, k := fixture()
			client := New(d, k, Options{MaxConcurrent: 1})
			permit, err := client.Acquire(context.Background(), "test", "work", kind)
			if err != nil || permit == nil || permit.PodIP != "10.0.0.23" {
				t.Fatalf("permit=%+v err=%v", permit, err)
			}
			if _, err := client.Acquire(context.Background(), "test", "work", kind); !errors.Is(err, ErrCapacity) {
				t.Fatalf("capacity: %v", err)
			}
			if err := permit.Release(context.Background()); err != nil {
				t.Fatal(err)
			}
			leases, err := k.CoordinationV1().Leases("test").List(context.Background(), metav1.ListOptions{})
			if err != nil || len(leases.Items) != 1 || leases.Items[0].Labels[StateLabel] != Released || leases.Items[0].Labels[EpochLabel] != "7" || leases.Items[0].Labels[KindLabel] != kind {
				t.Fatalf("completion ledger=%+v err=%v", leases, err)
			}
		})
	}
}

func TestPermitReleaseDeletesTicketAfterPolicyRemoval(t *testing.T) {
	d, k := fixture()
	permit, err := New(d, k, Options{}).Acquire(context.Background(), "test", "work", Request)
	if err != nil {
		t.Fatal(err)
	}
	sandbox, err := d.Resource(SandboxGVR).Namespace("test").Get(context.Background(), "work", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	unstructured.RemoveNestedField(sandbox.Object, "spec", "autoSuspension")
	if _, err := d.Resource(SandboxGVR).Namespace("test").Update(context.Background(), sandbox, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := permit.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	leases, err := k.CoordinationV1().Leases("test").List(context.Background(), metav1.ListOptions{})
	if err != nil || len(leases.Items) != 0 {
		t.Fatalf("tickets after policy removal=%+v err=%v", leases.Items, err)
	}
}

func TestGateClosingAfterTicketCreationCannotAdmit(t *testing.T) {
	for _, kind := range []string{Request, Background} {
		t.Run(kind, func(t *testing.T) {
			d, k := fixture()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			gets := 0
			d.PrependReactor("get", "sandboxes", func(_ ktesting.Action) (bool, runtime.Object, error) {
				gets++
				if gets != 2 {
					return false, nil, nil
				}
				leases, err := k.CoordinationV1().Leases("test").List(context.Background(), metav1.ListOptions{})
				if err != nil || len(leases.Items) != 1 || leases.Items[0].Labels[StateLabel] != Active {
					t.Fatalf("second read precedes durable ticket: %+v %v", leases, err)
				}
				obj, err := d.Tracker().Get(SandboxGVR, "test", "work")
				if err != nil {
					t.Fatal(err)
				}
				s := obj.(*unstructured.Unstructured).DeepCopy()
				_ = unstructured.SetNestedField(s.Object, "Draining", "status", "autoSuspension", "gate")
				cancel()
				return true, s, nil
			})
			permit, err := New(d, k, Options{PollInterval: time.Millisecond}).Acquire(ctx, "test", "work", kind)
			if permit != nil || !errors.Is(err, context.Canceled) {
				t.Fatalf("closed gate admitted: %+v %v", permit, err)
			}
			leases, _ := k.CoordinationV1().Leases("test").List(context.Background(), metav1.ListOptions{})
			for _, lease := range leases.Items {
				if lease.Labels[KindLabel] != Wake && lease.Labels[StateLabel] != Released {
					t.Fatalf("ungranted ticket remains Active: %+v", lease)
				}
			}
		})
	}
}

func TestPolicyRemovalDoesNotBypassGate(t *testing.T) {
	d, k := fixture()
	obj, _ := d.Tracker().Get(SandboxGVR, "test", "work")
	s := obj.(*unstructured.Unstructured)
	unstructured.RemoveNestedField(s.Object, "spec", "autoSuspension")
	_ = unstructured.SetNestedField(s.Object, "Hibernated", "status", "autoSuspension", "gate")
	_ = d.Tracker().Update(SandboxGVR, s, "test")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	permit, err := New(d, k, Options{PollInterval: time.Millisecond}).Acquire(ctx, "test", "work", Request)
	if permit != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("removed policy bypassed closed gate: %+v %v", permit, err)
	}
}

func TestLegacyAndManualModes(t *testing.T) {
	d, k := fixture()
	obj, _ := d.Tracker().Get(SandboxGVR, "test", "work")
	s := obj.(*unstructured.Unstructured)
	unstructured.RemoveNestedField(s.Object, "spec", "autoSuspension")
	unstructured.RemoveNestedField(s.Object, "status", "autoSuspension")
	_ = d.Tracker().Update(SandboxGVR, s, "test")
	permit, err := New(d, k, Options{}).Acquire(context.Background(), "test", "work", Request)
	if permit != nil || err != nil {
		t.Fatalf("legacy permit=%+v err=%v", permit, err)
	}
	d, k = fixture()
	obj, _ = d.Tracker().Get(SandboxGVR, "test", "work")
	s = obj.(*unstructured.Unstructured)
	_ = unstructured.SetNestedField(s.Object, "Suspended", "spec", "operatingMode")
	_ = d.Tracker().Update(SandboxGVR, s, "test")
	permit, err = New(d, k, Options{}).Acquire(context.Background(), "test", "work", Request)
	if permit != nil || !errors.Is(err, ErrManualMode) {
		t.Fatalf("manual permit=%+v err=%v", permit, err)
	}
}

func TestLeaseQuotaOrCompletionFailureDoesNotLoseProtection(t *testing.T) {
	t.Run("quota", func(t *testing.T) {
		d, k := fixture()
		k.PrependReactor("create", "leases", func(ktesting.Action) (bool, runtime.Object, error) {
			return true, nil, fmt.Errorf("exceeded quota: activity-leases")
		})
		permit, err := New(d, k, Options{}).Acquire(context.Background(), "test", "work", Request)
		if permit != nil || err == nil {
			t.Fatalf("quota bypass: %+v %v", permit, err)
		}
	})
	t.Run("completion unavailable", func(t *testing.T) {
		d, k := fixture()
		permit, err := New(d, k, Options{}).Acquire(context.Background(), "test", "work", Request)
		if err != nil {
			t.Fatal(err)
		}
		k.PrependReactor("update", "leases", func(ktesting.Action) (bool, runtime.Object, error) { return true, nil, fmt.Errorf("API unavailable") })
		if err := permit.Release(context.Background()); err == nil {
			t.Fatal("lost completion write not reported")
		}
		leases, _ := k.CoordinationV1().Leases("test").List(context.Background(), metav1.ListOptions{})
		if len(leases.Items) != 1 || leases.Items[0].Labels[StateLabel] != Active {
			t.Fatalf("failed completion lost protection: %+v", leases)
		}
	})
}

func TestManualRunningDoesNotBypassPendingCheckpointCancellation(t *testing.T) {
	for _, pending := range []string{"stale Ready", "checkpoint binding"} {
		t.Run(pending, func(t *testing.T) {
			d, k := fixture()
			obj, _ := d.Tracker().Get(SandboxGVR, "test", "work")
			s := obj.(*unstructured.Unstructured)
			if pending == "stale Ready" {
				s.SetGeneration(2)
			} else {
				_ = unstructured.SetNestedMap(s.Object, map[string]any{"sourcePodUID": "pod-uid"}, "status", "checkpointRestore")
			}
			_ = d.Tracker().Update(SandboxGVR, s, "test")
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancel()
			permit, err := New(d, k, Options{PollInterval: time.Millisecond}).Acquire(ctx, "test", "work", Request)
			if permit != nil || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("unfinished lifecycle admitted: %+v %v", permit, err)
			}
		})
	}
}

func TestPermitReportsEachAPIRequestExactlyOnce(t *testing.T) {
	d, k := fixture()
	var operations []string
	client := New(d, k, Options{ObserveAPICall: func(operation string, err error) {
		if err != nil {
			t.Errorf("unexpected API error for %s: %v", operation, err)
		}
		operations = append(operations, operation)
	}})
	permit, err := client.Acquire(context.Background(), "test", "work", Request)
	if err != nil {
		t.Fatal(err)
	}
	if err := permit.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{"SandboxGet", "LeaseCreate", "SandboxGet", "PodList", "LeaseGet", "SandboxGet", "LeaseUpdate"}
	if !reflect.DeepEqual(operations, want) {
		t.Fatalf("API calls=%v want=%v", operations, want)
	}
}
