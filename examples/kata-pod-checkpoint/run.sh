#!/usr/bin/env bash
# Copyright 2026 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Copyright 2026 The Kubernetes Authors.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

: "${KUBE_CONTEXT:?set an explicit test cluster context}"
: "${RUNTIME_CLASS:?set the Kata C/R RuntimeClass}"
: "${EVIDENCE_DIR:?set a new directory for run evidence}"
: "${E2E_NAMESPACE:?set the unused namespace reserved at preflight}"
: "${E2E_RUN_ID:?set the same run ID used at preflight}"

k=(kubectl --context "$KUBE_CONTEXT" --request-timeout=20s)
runtime="$RUNTIME_CLASS"
image="${PROBE_IMAGE:-docker.io/library/python:3.12-alpine}"
run="$E2E_RUN_ID"
ns="$E2E_NAMESPACE"
suite=agent-sandbox-pod-cr
name=continuity
if [[ ! "$ns" =~ ^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$ ||
      ! "$run" =~ ^[A-Za-z0-9]([A-Za-z0-9_.-]{0,61}[A-Za-z0-9])?$ ]]; then
  printf '%s\n' 'Invalid namespace or run label value.' >&2
  exit 2
fi
mkdir "$EVIDENCE_DIR"
trap 'status=$?; printf "namespace=%s run=%s evidence=%s exit=%d\n" "$ns" "$run" "$EVIDENCE_DIR" "$status"' EXIT
jq -n --arg context "$KUBE_CONTEXT" --arg ns "$ns" --arg run "$run" --arg suite "$suite" '
  {context:$context, namespace:$ns, run:$run, suite:$suite}' >"$EVIDENCE_DIR/identity.json"

"${k[@]}" explain pod.spec.restoreFrom >"$EVIDENCE_DIR/restore-schema.txt"
"${k[@]}" get runtimeclass "$runtime" -o json >"$EVIDENCE_DIR/runtimeclass.json"
"${k[@]}" get crd sandboxes.agents.x-k8s.io -o json |
  jq -e 'any(.spec.versions[]; .name == "v1beta1" and (.schema.openAPIV3Schema.properties.spec.properties.operatingMode.enum | index("Hibernated") != null))' >/dev/null
"${k[@]}" get podcheckpoints.checkpoint.k8s.io -A -o name >/dev/null
jq -n --arg ns "$ns" --arg run "$run" --arg suite "$suite" '
  {apiVersion:"v1", kind:"Namespace",
   metadata:{name:$ns,labels:{"agents.x-k8s.io/e2e-run":$run,"agents.x-k8s.io/e2e-suite":$suite}}}' |
  "${k[@]}" create -f - -o json >"$EVIDENCE_DIR/namespace.json"
k+=(-n "$ns")

probe='
import os, uuid
from http.server import HTTPServer, BaseHTTPRequestHandler

token = uuid.uuid4().hex
counter = 0
class Probe(BaseHTTPRequestHandler):
    def do_GET(self):
        global counter
        counter += 1
        disk = int(open("/probe-counter").read()) + 1 if os.path.exists("/probe-counter") else 1
        with open("/probe-counter", "w") as out:
            out.write(str(disk))
        self.send_response(200)
        self.end_headers()
        self.wfile.write(f"{token} {counter} {disk}\n".encode())
HTTPServer(("127.0.0.1", 8080), Probe).serve_forever()
'
jq -n --arg name "$name" --arg runtime "$runtime" --arg image "$image" --arg probe "$probe" --arg run "$run" --arg suite "$suite" '
  {"agents.x-k8s.io/e2e-run":$run,"agents.x-k8s.io/e2e-suite":$suite} as $labels |
  {apiVersion:"agents.x-k8s.io/v1beta1", kind:"Sandbox",
   metadata:{name:$name,labels:$labels},
   spec:{operatingMode:"Running",podTemplate:{metadata:{labels:$labels},
    spec:{runtimeClassName:$runtime,hostNetwork:false,automountServiceAccountToken:false,
      containers:[{name:"probe",image:$image,command:["python","-u","-c",$probe],
        readinessProbe:{exec:{command:["python","-c","import socket; socket.create_connection((\"127.0.0.1\",8080),2)"]}}}]}}}}' |
  "${k[@]}" create -f - -o json >"$EVIDENCE_DIR/sandbox.json"
sandbox_uid=$(jq -er '.metadata.uid | strings | select(length > 0)' "$EVIDENCE_DIR/sandbox.json")

wait_condition() {
  local condition=$1
  for ((i=0; i<300; i++)); do
    if "${k[@]}" get sandbox "$name" -o json | jq -e --arg condition "$condition" '
      .metadata.generation as $generation |
      any(.status.conditions[]?; .type == $condition and .status == "True" and .observedGeneration == $generation)' >/dev/null; then
      return 0
    fi
    sleep 2
  done
  return 1
}

sample() {
  "${k[@]}" exec "$name" -c probe -- python -c \
    'import urllib.request; print(urllib.request.urlopen("http://127.0.0.1:8080", timeout=3).read().decode().strip())'
}

wait_condition Ready
for cycle in 1 2; do
  "${k[@]}" get pod "$name" -o json >"$EVIDENCE_DIR/source-$cycle.json"
  source_uid=$(jq -er --arg owner "$sandbox_uid" '
    select(any(.metadata.ownerReferences[]?; .controller and .uid == $owner)) |
    select(.status.podIP != null and .status.podIP != "") | .metadata.uid' "$EVIDENCE_DIR/source-$cycle.json")
  before=$(sample)
  printf '%s\n' "$before" >"$EVIDENCE_DIR/before-$cycle.txt"
  "${k[@]}" patch sandbox "$name" --type merge -p '{"spec":{"operatingMode":"Hibernated"}}'
  wait_condition Hibernated
  "${k[@]}" get sandbox "$name" -o json >"$EVIDENCE_DIR/hibernated-$cycle.json"
  checkpoint=$(jq -er '.status.checkpointRestore.checkpointName' "$EVIDENCE_DIR/hibernated-$cycle.json")
  "${k[@]}" get podcheckpoint "$checkpoint" -o json >"$EVIDENCE_DIR/checkpoint-$cycle.json"
  jq -e --arg uid "$source_uid" --arg owner "$sandbox_uid" '
    .spec.sourcePod.uid == $uid and
    any(.metadata.ownerReferences[]?; .controller and .uid == $owner) and
    any(.status.conditions[]?; .type == "Ready" and .status == "True")' "$EVIDENCE_DIR/checkpoint-$cycle.json" >/dev/null
  [[ -z $("${k[@]}" get pod "$name" --ignore-not-found -o name) ]]

  "${k[@]}" patch sandbox "$name" --type merge -p '{"spec":{"operatingMode":"Running"}}'
  wait_condition RestoreReady
  wait_condition Ready
  "${k[@]}" get pod "$name" -o json >"$EVIDENCE_DIR/restore-$cycle.json"
  jq -e --arg source "$source_uid" --arg checkpoint "$checkpoint" --arg owner "$sandbox_uid" '
    .metadata.uid != $source and .spec.restoreFrom.name == $checkpoint and
    (.status.podIP // "") != "" and
    any(.metadata.ownerReferences[]?; .controller and .uid == $owner)' "$EVIDENCE_DIR/restore-$cycle.json" >/dev/null
  "${k[@]}" get pods -o json | jq -e '
    [.items[]] | length == 1' >/dev/null
  after=$(sample)
  printf '%s\n' "$after" >"$EVIDENCE_DIR/after-$cycle.txt"
  read -r before_token before_mem before_disk <<<"$before"
  read -r after_token after_mem after_disk <<<"$after"
  [[ "$before_token" == "$after_token" && "$after_mem" -gt "$before_mem" && "$after_disk" -gt "$before_disk" ]]
done

printf '%s\n' 'Two-cycle Kubernetes success-path assertions passed.'
