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
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"google.golang.org/grpc/interop/grpc_testing"
	"google.golang.org/grpc/metadata"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/agent-sandbox/internal/activity"
	"sigs.k8s.io/agent-sandbox/sandbox-router/cache"
	"sigs.k8s.io/agent-sandbox/sandbox-router/config"
)

func TestActivityPreservesPathRoutingAndCredentialStripping(t *testing.T) {
	admission, _ := activityFixture()
	cfg := config.Defaults()
	cfg.PathRoutingPrefix = "/sb"
	cfg.AuthzCookieQueryParam = "auth_token"
	h := NewHandler(Options{Config: &cfg, Activity: admission, Logger: logr.Discard()})
	t.Cleanup(h.Close)
	calls := 0
	h.directTransport = activityTransport(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.URL.Host != "10.0.0.23:8888" || r.URL.EscapedPath() != "/a%2Fb" || r.URL.RawQuery != "x=y" {
			t.Fatalf("admission lost routed URL: %s", r.URL)
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("ok"))}, nil
	})
	r := httptest.NewRequest(http.MethodPost, "http://router/sb/test/work/8888/a%2Fb?auth_token=secret&x=y", strings.NewReader("body"))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK || calls != 1 {
		t.Fatalf("status=%d calls=%d body=%s", w.Code, calls, w.Body.String())
	}
}

func TestActivityRejectsReplacedSandboxBeforeCreatingTickets(t *testing.T) {
	admission, k := activityFixture()
	cfg := config.Defaults()
	oldUID := types.UID("old-sandbox-uid")
	lookup := &stubLookup{entries: map[types.UID]cache.Entry{oldUID: {SandboxUID: oldUID, SandboxName: "work", Namespace: "test", PodIP: "10.9.9.9"}}}
	h := NewHandler(Options{Config: &cfg, Activity: admission, Cache: lookup, Logger: logr.Discard()})
	t.Cleanup(h.Close)
	h.directTransport = activityTransport(func(*http.Request) (*http.Response, error) {
		t.Fatal("replaced Sandbox received a request")
		return nil, io.EOF
	})
	r := httptest.NewRequest(http.MethodPost, "http://router/write", nil)
	r.Header.Set(HeaderSandboxID, "work")
	r.Header.Set(HeaderSandboxNamespace, "test")
	r.Header.Set(HeaderSandboxPort, "8888")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusForbidden {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	leases, err := k.CoordinationV1().Leases("test").List(context.Background(), metav1.ListOptions{})
	if err != nil || len(leases.Items) != 0 {
		t.Fatalf("replaced Sandbox was woken/admitted: %+v %v", leases, err)
	}
}

func TestActivityPreservesNativeGRPCTransportAndCompletion(t *testing.T) {
	admission, k := activityFixture()
	cfg := config.Defaults()
	cfg.UpstreamMaxRetries = 3
	h := NewHandler(Options{Config: &cfg, Activity: admission, Logger: logr.Discard()})
	t.Cleanup(h.Close)
	frame := []byte{0, 0, 0, 0, 0}
	calls := 0
	h.directTransport = activityTransport(func(*http.Request) (*http.Response, error) {
		t.Fatal("gRPC used the HTTP/1 direct transport")
		return nil, io.EOF
	})
	h.directGRPCTransport = activityTransport(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.URL.Host != "10.0.0.23:8888" || r.Header.Get("Content-Type") != "application/grpc" {
			t.Fatalf("invalid admitted RPC: %s %+v", r.URL, r.Header)
		}
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/grpc"}}, Trailer: http.Header{"Grpc-Status": []string{"0"}}, Body: io.NopCloser(bytes.NewReader(frame))}, nil
	})
	r := httptest.NewRequest(http.MethodPost, "http://router/service/Method", bytes.NewReader(frame))
	r.Proto = "HTTP/2.0"
	r.ProtoMajor = 2
	r.ProtoMinor = 0
	r.Header.Set("Content-Type", "application/grpc")
	r.Header.Set(HeaderSandboxID, "work")
	r.Header.Set(HeaderSandboxNamespace, "test")
	r.Header.Set(HeaderSandboxPort, "8888")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK || calls != 1 || !bytes.Equal(w.Body.Bytes(), frame) {
		t.Fatalf("RPC status=%d calls=%d body=%v", w.Code, calls, w.Body.Bytes())
	}
	leases, err := k.CoordinationV1().Leases("test").List(context.Background(), metav1.ListOptions{})
	if err != nil || len(leases.Items) != 1 || leases.Items[0].Labels[activity.StateLabel] != activity.Released {
		t.Fatalf("RPC completion not recorded: %+v %v", leases, err)
	}
}

func TestActivityNativeGRPCOverHTTP2(t *testing.T) {
	host, port := grpcBackend(t, &grpcEchoService{})
	admission, k := activityFixture()
	pod, err := k.CoreV1().Pods("test").Get(context.Background(), "work", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pod.Status.PodIP = host
	if _, err = k.CoreV1().Pods("test").UpdateStatus(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	cfg := config.Defaults()
	cfg.AllowLoopbackPodIP = true
	cfg.UpstreamMaxRetries = 3
	rpc := grpc_testing.NewTestServiceClient(grpcRouterConn(t, Options{Config: &cfg, Activity: admission}, false))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("x-sandbox-id", "work", "x-sandbox-namespace", "test", "x-sandbox-port", port))
	reply, err := rpc.UnaryCall(ctx, &grpc_testing.SimpleRequest{Payload: &grpc_testing.Payload{Body: []byte("admitted-h2")}})
	if err != nil || string(reply.GetPayload().GetBody()) != "admitted-h2" {
		t.Fatalf("native admitted RPC: %+v %v", reply, err)
	}
	until := time.Now().Add(time.Second)
	for {
		leases, err := k.CoordinationV1().Leases("test").List(context.Background(), metav1.ListOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if len(leases.Items) == 1 && leases.Items[0].Labels[activity.StateLabel] == activity.Released {
			break
		}
		if time.Now().After(until) {
			t.Fatalf("native RPC left active admission: %+v", leases)
		}
		time.Sleep(time.Millisecond)
	}
}
