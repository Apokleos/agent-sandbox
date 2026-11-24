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

// Package activity implements durable admission tickets for automatic hibernation.
// Background workers must acquire before starting work and release only after work
// really ends. Lease expiry never grants permission to checkpoint active work.
package activity

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

const (
	SandboxUIDLabel = "agents.x-k8s.io/activity-sandbox-uid"
	KindLabel       = "agents.x-k8s.io/activity-kind"
	StateLabel      = "agents.x-k8s.io/activity-state"
	EpochLabel      = "agents.x-k8s.io/activity-epoch"
	Request         = "Request"
	Background      = "Background"
	Wake            = "Wake"
	Active          = "Active"
	Released        = "Released"
)

var (
	SandboxGVR         = schema.GroupVersionResource{Group: "agents.x-k8s.io", Version: "v1beta1", Resource: "sandboxes"}
	ErrCapacity        = errors.New("activity admission capacity exhausted")
	ErrSandboxReplaced = errors.New("sandbox UID changed during activity admission")
	ErrManualMode      = errors.New("sandbox is manually stopped or expired")
)

type Options struct {
	Identity      string
	PollInterval  time.Duration
	WaitTimeout   time.Duration
	MaxConcurrent int
	// ObserveAPICall is invoked once after each API request, including failures.
	// Operation is one of SandboxGet, LeaseCreate, LeaseGet, LeaseUpdate,
	// LeaseDelete, PodList. Implementations must be safe for concurrent calls.
	ObserveAPICall func(operation string, err error)
}

// Client uses direct API clients: cached reads cannot implement the admission fence.
type Client struct {
	dynamic        dynamic.Interface
	kube           kubernetes.Interface
	identity       string
	poll           time.Duration
	timeout        time.Duration
	slots          chan struct{}
	observeAPICall func(operation string, err error)
}

func New(d dynamic.Interface, k kubernetes.Interface, o Options) *Client {
	if o.Identity == "" {
		o.Identity = "router-" + string(uuid.NewUUID())
	}
	if o.PollInterval <= 0 {
		o.PollInterval = 500 * time.Millisecond
	}
	if o.WaitTimeout <= 0 {
		o.WaitTimeout = 10 * time.Minute
	}
	if o.MaxConcurrent <= 0 {
		o.MaxConcurrent = 64
	}
	return &Client{dynamic: d, kube: k, identity: o.Identity, poll: o.PollInterval, timeout: o.WaitTimeout, slots: make(chan struct{}, o.MaxConcurrent), observeAPICall: o.ObserveAPICall}
}

func (c *Client) observe(operation string, err error) {
	if c.observeAPICall != nil {
		c.observeAPICall(operation, err)
	}
}

// Permit must outlive the entire upstream request, including upgraded streams.
type Permit struct {
	PodIP      string
	SandboxUID types.UID
	client     *Client
	ticket     *coordinationv1.Lease
}

// Release records durable completion. Failure deliberately leaves the ticket
// Active, blocking suspension until an operator can prove the work has ended.
// Call it once, with a fresh bounded context after request cancellation.
func (p *Permit) Release(ctx context.Context) error {
	if p == nil {
		return nil
	}
	defer func() { <-p.client.slots }()
	return p.client.release(ctx, p.ticket)
}

func optedIn(s *unstructured.Unstructured) bool {
	if checkpointRestoreUnsupported(s) {
		return false
	}
	_, spec, _ := unstructured.NestedMap(s.Object, "spec", "autoSuspension")
	if spec {
		return true
	}
	gate, status, _ := unstructured.NestedString(s.Object, "status", "autoSuspension", "gate")
	return status && gate != "" && gate != "Open"
}

func checkpointRestoreUnsupported(s *unstructured.Unstructured) bool {
	conditions, _, _ := unstructured.NestedSlice(s.Object, "status", "conditions")
	for _, entry := range conditions {
		condition, ok := entry.(map[string]any)
		if !ok || condition["type"] != "CheckpointRestoreSupported" || condition["status"] != "False" || condition["reason"] != "Unsupported" {
			continue
		}
		generation, _, _ := unstructured.NestedInt64(condition, "observedGeneration")
		return generation == s.GetGeneration()
	}
	return false
}

func allowed(s *unstructured.Unstructured) bool {
	if !s.GetDeletionTimestamp().IsZero() {
		return false
	}
	mode, _, _ := unstructured.NestedString(s.Object, "spec", "operatingMode")
	if mode != "" && mode != "Running" {
		return false
	}
	expires, _, _ := unstructured.NestedString(s.Object, "spec", "shutdownTime")
	if expires != "" {
		t, err := time.Parse(time.RFC3339, expires)
		if err != nil || !time.Now().Before(t) {
			return false
		}
	}
	return true
}

func gate(s *unstructured.Unstructured) (string, int64) {
	g, _, _ := unstructured.NestedString(s.Object, "status", "autoSuspension", "gate")
	e, _, _ := unstructured.NestedInt64(s.Object, "status", "autoSuspension", "epoch")
	return g, e
}

func admissionReady(s *unstructured.Unstructured) bool {
	conditions, _, _ := unstructured.NestedSlice(s.Object, "status", "conditions")
	ready, restored := false, false
	for _, entry := range conditions {
		condition, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		generation, _, _ := unstructured.NestedInt64(condition, "observedGeneration")
		if condition["status"] != "True" || generation != s.GetGeneration() {
			continue
		}
		switch condition["type"] {
		case "Ready":
			ready = true
		case "RestoreReady":
			restored = true
		}
	}
	_, bound, _ := unstructured.NestedMap(s.Object, "status", "checkpointRestore")
	return ready && (!bound || restored)
}

func (c *Client) read(ctx context.Context, namespace, name string) (*unstructured.Unstructured, error) {
	sandbox, err := c.dynamic.Resource(SandboxGVR).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	c.observe("SandboxGet", err)
	return sandbox, err
}

func (c *Client) ticket(ctx context.Context, s *unstructured.Unstructured, kind string, epoch int64) (*coordinationv1.Lease, error) {
	controller := true
	ticket, err := c.kube.CoordinationV1().Leases(s.GetNamespace()).Create(ctx, &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name: "as-activity-" + string(uuid.NewUUID()), Namespace: s.GetNamespace(),
			Labels:          map[string]string{SandboxUIDLabel: string(s.GetUID()), KindLabel: kind, StateLabel: Active, EpochLabel: strconv.FormatInt(epoch, 10)},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: SandboxGVR.GroupVersion().String(), Kind: "Sandbox", Name: s.GetName(), UID: s.GetUID(), Controller: &controller}},
		},
		Spec: coordinationv1.LeaseSpec{HolderIdentity: &c.identity},
	}, metav1.CreateOptions{})
	c.observe("LeaseCreate", err)
	return ticket, err
}

func (c *Client) release(ctx context.Context, ticket *coordinationv1.Lease) error {
	leases := c.kube.CoordinationV1().Leases(ticket.Namespace)
	current, err := leases.Get(ctx, ticket.Name, metav1.GetOptions{})
	c.observe("LeaseGet", err)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read completion ticket: %w", err)
	}
	if current.UID != ticket.UID {
		return fmt.Errorf("completion ticket UID changed")
	}
	if owner := metav1.GetControllerOf(current); owner != nil && owner.Kind == "Sandbox" {
		sandbox, readErr := c.read(ctx, current.Namespace, owner.Name)
		if apierrors.IsNotFound(readErr) {
			return c.deleteTicket(ctx, current)
		}
		if readErr != nil {
			return fmt.Errorf("read Sandbox before recording completion: %w", readErr)
		}
		if sandbox.GetUID() != owner.UID || !optedIn(sandbox) {
			return c.deleteTicket(ctx, current)
		}
	}
	current.Labels[StateLabel] = Released
	now := metav1.NewMicroTime(time.Now())
	current.Spec.RenewTime = &now
	_, err = leases.Update(ctx, current, metav1.UpdateOptions{})
	c.observe("LeaseUpdate", err)
	return err
}

func (c *Client) deleteTicket(ctx context.Context, ticket *coordinationv1.Lease) error {
	uid := ticket.UID
	err := c.kube.CoordinationV1().Leases(ticket.Namespace).Delete(ctx, ticket.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}})
	c.observe("LeaseDelete", err)
	return clientIgnoreNotFound(err)
}

func clientIgnoreNotFound(err error) error {
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

// Acquire returns nil for legacy Sandboxes. For opt-in workloads it grants a
// ticket only after a create-then-read fence and authoritative Pod readiness.
func (c *Client) Acquire(ctx context.Context, namespace, name, kind string) (*Permit, error) {
	return c.AcquireForSandbox(ctx, namespace, name, kind, "")
}

// AcquireForSandbox rejects a replaced Sandbox before creating wake or request
// tickets. An empty expectedUID preserves name-scoped admission.
func (c *Client) AcquireForSandbox(ctx context.Context, namespace, name, kind string, expectedUID types.UID) (_ *Permit, resultErr error) {
	if kind != Request && kind != Background {
		return nil, fmt.Errorf("unsupported activity kind %q", kind)
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	s, err := c.read(ctx, namespace, name)
	if err != nil {
		return nil, fmt.Errorf("read Sandbox activity policy: %w", err)
	}
	if expectedUID != "" && s.GetUID() != expectedUID {
		return nil, ErrSandboxReplaced
	}
	if !optedIn(s) {
		return nil, nil
	}
	select {
	case c.slots <- struct{}{}:
	default:
		return nil, ErrCapacity
	}
	granted := false
	defer func() {
		if !granted {
			<-c.slots
		}
	}()
	uid := s.GetUID()
	var wake *coordinationv1.Lease
	defer func() {
		if wake != nil {
			cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			if err := c.deleteTicket(cleanup, wake); err != nil && resultErr == nil {
				resultErr = fmt.Errorf("release wake demand: %w", err)
			}
		}
	}()
	for {
		if checkpointRestoreUnsupported(s) {
			return nil, nil
		}
		if s.GetUID() != uid {
			return nil, ErrSandboxReplaced
		}
		if !allowed(s) {
			return nil, ErrManualMode
		}
		g, epoch := gate(s)
		// Waiting for provisioning or a new Ready generation is wake demand,
		// not an application request. Avoid churning ungranted completions.
		if g == "Open" && admissionReady(s) {
			ticket, err := c.ticket(ctx, s, kind, epoch)
			if err != nil {
				return nil, fmt.Errorf("create activity ticket: %w", err)
			}
			check, err := c.read(ctx, namespace, name)
			if err == nil {
				s = check
				g2, e2 := gate(check)
				if !checkpointRestoreUnsupported(check) && check.GetUID() == uid && g2 == "Open" && e2 == epoch && allowed(check) && admissionReady(check) {
					ip, readyErr := c.readyPodIP(ctx, namespace, uid)
					if readyErr == nil && ip != "" {
						if wake != nil {
							if cleanupErr := c.deleteTicket(ctx, wake); cleanupErr != nil {
								cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
								_ = c.release(cleanup, ticket)
								stop()
								return nil, fmt.Errorf("consume wake demand: %w", cleanupErr)
							}
							wake = nil
						}
						granted = true
						return &Permit{PodIP: ip, SandboxUID: uid, client: c, ticket: ticket}, nil
					}
					if readyErr != nil {
						err = readyErr
					}
				}
			}
			cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
			releaseErr := c.release(cleanup, ticket)
			stop()
			if releaseErr != nil {
				return nil, fmt.Errorf("release ungranted ticket: %w", releaseErr)
			}
			if err != nil {
				return nil, fmt.Errorf("confirm activity admission: %w", err)
			}
			if checkpointRestoreUnsupported(s) {
				return nil, nil
			}
		}
		if wake == nil {
			wake, err = c.ticket(ctx, s, Wake, epoch)
			if err != nil {
				return nil, fmt.Errorf("create wake demand: %w", err)
			}
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(c.poll):
		}
		s, err = c.read(ctx, namespace, name)
		if err != nil {
			return nil, fmt.Errorf("wait for Sandbox admission: %w", err)
		}
	}
}

func (c *Client) readyPodIP(ctx context.Context, namespace string, uid types.UID) (string, error) {
	pods, err := c.kube.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	c.observe("PodList", err)
	if err != nil {
		return "", err
	}
	var owned []*corev1.Pod
	for i := range pods.Items {
		p := &pods.Items[i]
		if owner := metav1.GetControllerOf(p); owner != nil && owner.UID == uid && owner.Kind == "Sandbox" {
			owned = append(owned, p)
		}
	}
	if len(owned) > 1 {
		return "", fmt.Errorf("multiple Sandbox-owned Pods block admission")
	}
	if len(owned) == 0 {
		return "", nil
	}
	p := owned[0]
	if !p.DeletionTimestamp.IsZero() || p.Status.Phase != corev1.PodRunning {
		return "", nil
	}
	for _, condition := range p.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return p.Status.PodIP, nil
		}
	}
	return "", nil
}
