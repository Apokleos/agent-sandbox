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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

type SnapshotState string

const (
	SnapshotPending SnapshotState = "Pending"
	SnapshotReady   SnapshotState = "Ready"
	SnapshotFailed  SnapshotState = "Failed"
	SnapshotUnknown SnapshotState = "Unknown"
	SnapshotAbsent  SnapshotState = "Absent"
)

type SnapshotSupport string

const (
	SnapshotSupported   SnapshotSupport = "Supported"
	SnapshotUnsupported SnapshotSupport = "Unsupported"
	SnapshotUnverified  SnapshotSupport = "Unknown"
)

type SnapshotCapabilities struct {
	MemoryState      SnapshotSupport
	ProcessState     SnapshotSupport
	WritableRootFS   SnapshotSupport
	RestoreRequired  SnapshotSupport
	SameNodeRequired SnapshotSupport
}

type SnapshotObjectRef struct {
	Resource  schema.GroupVersionResource
	Kind      string
	Namespace string
	Name      string
	UID       types.UID
}

// Request and Artifact coincide for Kata. They are distinct for GKE.
type SnapshotBinding struct {
	ProviderID  string
	OperationID string
	Request     SnapshotObjectRef
	Artifact    *SnapshotObjectRef
}

type SnapshotCheckpointInput struct {
	OperationID    string
	RequestName    string
	RequestMeta    metav1.ObjectMeta
	SourcePod      *corev1.Pod
	TemplateDigest string
}

type SnapshotObjectEvidence struct {
	Ref               SnapshotObjectRef
	ControllerOwner   *metav1.OwnerReference
	TemplateDigest    string
	DeletionTimestamp *metav1.Time
}

// ObservedSourceUID is extracted from backend data, never copied from intent.
type SnapshotCheckpointObservation struct {
	Binding                SnapshotBinding
	State                  SnapshotState
	Request                SnapshotObjectEvidence
	Artifact               *SnapshotObjectEvidence
	ObservedSourceName     string
	ObservedSourceUID      types.UID
	SourceNode             string
	HasCheckpointedPodSpec bool
	Reason                 string
	Message                string
}

// This is data, not Pod CRUD. Dynamic data preserves nonstandard restore fields.
// Core owns the final name, namespace, owner, tracking labels, and attempt.
type SnapshotRestorePodPlan struct {
	Pod        *unstructured.Unstructured
	Artifact   SnapshotObjectRef
	SourceNode string
}

// Ready means backend selection matched. Core must ALSO verify Pod ownership,
// attempt, exact UID and readiness. This is not runtime attestation.
type SnapshotRestoreProof struct {
	State    SnapshotState
	PodUID   types.UID
	Artifact *SnapshotObjectRef
	Reason   string
	Message  string
}

type SnapshotAssetDisposition string

const (
	SnapshotAssetsUnknown  SnapshotAssetDisposition = "Unknown"
	SnapshotAssetsRetained SnapshotAssetDisposition = "Retained"
	SnapshotAssetsDeleted  SnapshotAssetDisposition = "Deleted"
)

type SnapshotCleanupObservation struct {
	RequestGone      bool
	ArtifactGone     bool // API resource only; not node files
	AssetDisposition SnapshotAssetDisposition
	Reason           string
}

// SnapshotProvider has no Pod mutations and no Sandbox writes. Production
// currently selects only Kata; adding another backend needs durable selection.
type SnapshotProvider interface {
	ID() string
	Capabilities(context.Context, *corev1.Pod) (SnapshotCapabilities, error)
	EnsureCheckpoint(context.Context, SnapshotCheckpointInput) (SnapshotCheckpointObservation, error)
	ObserveCheckpoint(context.Context, SnapshotBinding) (SnapshotCheckpointObservation, error)
	ListCheckpoints(context.Context, string, metav1.ListOptions) ([]SnapshotCheckpointObservation, error)
	PrepareRestore(context.Context, SnapshotBinding) (SnapshotRestorePodPlan, error)
	ObserveRestore(context.Context, SnapshotBinding, SnapshotObjectRef) (SnapshotRestoreProof, error)
	DeleteCheckpoint(context.Context, SnapshotBinding) (SnapshotCleanupObservation, error)
}
