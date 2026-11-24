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

package proxy

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"

	"sigs.k8s.io/agent-sandbox/internal/activity"
	"sigs.k8s.io/agent-sandbox/sandbox-router/config"
)

type activityTransport func(*http.Request) (*http.Response, error)

func (f activityTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func activityFixture() (*activity.Client, *kubefake.Clientset) {
	s := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "agents.x-k8s.io/v1beta1", "kind": "Sandbox",
		"metadata": map[string]any{"name": "work", "namespace": "test", "uid": "sandbox-uid", "generation": int64(1)},
		"spec":     map[string]any{"operatingMode": "Running", "autoSuspension": map[string]any{}},
		"status":   map[string]any{"autoSuspension": map[string]any{"gate": "Open", "epoch": int64(1)}, "conditions": []any{map[string]any{"type": "Ready", "status": "True", "observedGeneration": int64(1)}}},
	}}
	controller := true
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "work", Namespace: "test", UID: "pod-uid", OwnerReferences: []metav1.OwnerReference{{APIVersion: s.GetAPIVersion(), Kind: "Sandbox", Name: s.GetName(), UID: s.GetUID(), Controller: &controller}}}, Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0.23", Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
	k := kubefake.NewClientset(pod)
	d := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	if _, err := d.Resource(activity.SandboxGVR).Namespace("test").Create(context.Background(), s, metav1.CreateOptions{}); err != nil {
		panic(err)
	}
	return activity.New(d, k, activity.Options{}), k
}

func TestActivityOverridesStaleEndpointAndDisablesReplay(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "dial failure"}[fail], func(t *testing.T) {
			client, k := activityFixture()
			cfg := config.Defaults()
			cfg.UpstreamMaxRetries = 3
			h := NewHandler(Options{Config: &cfg, Activity: client, Logger: logr.Discard()})
			calls := 0
			h.directTransport = activityTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				if r.Method != http.MethodPost || r.URL.Host != "10.0.0.23:8888" {
					t.Fatalf("stale target used: %s %s", r.Method, r.URL.Host)
				}
				leases, err := k.CoordinationV1().Leases("test").List(context.Background(), metav1.ListOptions{})
				if err != nil || len(leases.Items) != 1 || leases.Items[0].Labels[activity.StateLabel] != activity.Active {
					t.Fatalf("request forwarded without Active ticket: %+v %v", leases, err)
				}
				if fail {
					return nil, &net.OpError{Op: "dial", Net: "tcp", Err: io.EOF}
				}
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("ok"))}, nil
			})
			h.transport = activityTransport(func(*http.Request) (*http.Response, error) {
				t.Fatal("opt-in request used legacy retry transport")
				return nil, io.EOF
			})
			r := httptest.NewRequest(http.MethodPost, "http://router/write", strings.NewReader("mutation"))
			r.Header.Set(HeaderSandboxID, "work")
			r.Header.Set(HeaderSandboxNamespace, "test")
			r.Header.Set(HeaderSandboxPodIP, "10.9.9.9")
			r.Header.Set(HeaderSandboxUID, "old-pod-cache-key")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if calls != 1 {
				t.Fatalf("POST attempted %d times", calls)
			}
			want := http.StatusOK
			if fail {
				want = http.StatusBadGateway
			}
			if w.Code != want {
				t.Fatalf("status=%d want=%d", w.Code, want)
			}
			leases, _ := k.CoordinationV1().Leases("test").List(context.Background(), metav1.ListOptions{})
			if len(leases.Items) != 1 || leases.Items[0].Labels[activity.StateLabel] != activity.Released {
				t.Fatalf("request completion lost: %+v", leases)
			}
		})
	}
}
