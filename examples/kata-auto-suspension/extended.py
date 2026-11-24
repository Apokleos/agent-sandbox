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
"""Follow successful run.py in the same window: policy exit and cost baselines."""
import copy
import json
import os
import subprocess
from pathlib import Path
import time
import run as fixture

previous = Path(os.environ["PREVIOUS_EVIDENCE_DIR"])
original = json.loads((previous / "sandbox.json").read_text())
namespace = json.loads((previous / "namespace.json").read_text())
assert namespace["metadata"]["name"] == fixture.NS
assert namespace["metadata"]["labels"] == fixture.kube("get", "namespace", fixture.NS, "-o", "json", namespace=False)["metadata"]["labels"]
# Use the existing scoped secret in memory; do not export it to evidence.
import base64
fixture.SECRET = base64.b64decode(fixture.kube("get", "secret", "router-auth", "-o", "json")["data"]["key"]).decode()


def remove(name, uid):
    current = fixture.kube("get", "sandbox", name, "-o", "json")
    assert current["metadata"]["uid"] == uid
    assert current["metadata"]["labels"]["agents.x-k8s.io/e2e-run"] == fixture.RUN
    fixture.kube("delete", "--raw", f"/apis/agents.x-k8s.io/v1beta1/namespaces/{fixture.NS}/sandboxes/{name}", "-f", "-",
        obj={"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": {"uid": uid}})
    fixture.wait("Sandbox and workload cleanup " + name, lambda: not fixture.kube("get", "sandboxes", "-o", "json")["items"] and not fixture.pods())
    assert not fixture.kube("get", "podcheckpoints", "-o", "json")["items"]


def ready():
    sandbox = fixture.sandbox()
    return any(c["type"] == "Ready" and c["status"] == "True" and c["observedGeneration"] == sandbox["metadata"]["generation"] for c in sandbox.get("status", {}).get("conditions", []))


def create(name, policy):
    fixture.NAME = name
    spec = copy.deepcopy(original["spec"])
    spec["operatingMode"] = "Running"
    if policy:
        spec["autoSuspension"] = {"idleTimeoutSeconds": 5, "maxCycles": 1, "activityMode": "RoutedRequestsAndLeases"}
    else:
        spec.pop("autoSuspension", None)
        spec["service"] = True
    return fixture.create({"apiVersion": "agents.x-k8s.io/v1beta1", "kind": "Sandbox", "metadata": {"name": name, "labels": fixture.LABELS}, "spec": spec}, name + "-sandbox")


def enable_rejected(label):
    result = subprocess.run(fixture.KUBE + ["-n", fixture.NS, "patch", "sandbox", fixture.NAME, "--type=merge", "-p",
        '{"spec":{"autoSuspension":{"idleTimeoutSeconds":5,"maxCycles":1,"activityMode":"RoutedRequestsAndLeases"}}}'],
        text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    assert result.returncode != 0 and "autoSuspension must be enabled" in result.stderr, result.stderr
    (fixture.ROOT / (label + ".txt")).write_text(result.stderr)
    assert "autoSuspension" not in fixture.sandbox()["spec"]


try:
    assert json.loads((previous / "result.json").read_text())["result"] == "PASS"
    remove(original["metadata"]["name"], original["metadata"]["uid"])
    exiting = create("policy-exit", True)
    fixture.wait("policy-exit Ready", ready)
    first, _ = fixture.request("/increment", "POST")
    fixture.wait("policy-exit Checkpointing", lambda: fixture.gate("Checkpointing"))
    fixture.kube("patch", "sandbox", fixture.NAME, "--type=merge", "-p", '{"spec":{"autoSuspension":null}}', "-o", "json")
    fixture.wait("policy removal safely restored", lambda: fixture.gate("Open") and ready(), timeout=180)
    second, latency = fixture.request("/increment", "POST")
    assert second["token"] == first["token"] and second["memory"] == first["memory"] + 1 and second["disk"] == first["disk"] + 1
    assert fixture.sandbox()["status"]["autoSuspension"]["cycles"] == 1
    fixture.snapshot("policy-removed-restored")
    fixture.no_checkpoint(8)
    enable_rejected("reenable-policy-rejected")
    remove(fixture.NAME, exiting["metadata"]["uid"])
    start = time.monotonic()
    baseline = create("baseline", False)
    enable_rejected("live-policy-enable-rejected")
    fixture.wait("AlwaysRunning Ready", ready)
    warm, warm_latency = fixture.request("/increment", "POST")
    initial_cold = time.monotonic() - start
    fixture.host_sample("always-running")
    fixture.no_checkpoint(13)
    fixture.host_sample("always-running-after-idle")
    fixture.kube("patch", "sandbox", fixture.NAME, "--type=merge", "-p", '{"spec":{"operatingMode":"Suspended"}}', "-o", "json")
    fixture.wait("baseline suspended", lambda: not fixture.pods())
    start = time.monotonic()
    fixture.kube("patch", "sandbox", fixture.NAME, "--type=merge", "-p", '{"spec":{"operatingMode":"Running"}}', "-o", "json")
    fixture.wait("baseline cold restarted Ready", ready)
    cold, cold_latency = fixture.request("/increment", "POST")
    cold_resume = time.monotonic() - start
    assert cold["token"] != warm["token"] and cold["memory"] == 1 and cold["disk"] == 1
    fixture.host_sample("cold-restarted")
    fixture.save("result.json", {"result": "PASS", "runtimeClass": fixture.RUNTIME,
        "cases": ["policy-removal-during-checkpoint", "live-policy-enable-rejected", "reenable-policy-rejected", "AlwaysRunning-idle", "cold-recreate-state-loss"],
        "initialColdToResponseSeconds": initial_cold, "coldResumeToResponseSeconds": cold_resume,
        "steadyRouterRequestSeconds": warm_latency, "coldReadyRouterRequestSeconds": cold_latency,
        "sampleCount": 1, "note": "Pilot comparison; startup includes API/readiness polling, not a statistical latency claim."})
    remove(fixture.NAME, baseline["metadata"]["uid"])
    print("PASS: policy exit and cost baselines", flush=True)
except BaseException as error:
    fixture.save("failure.json", {"error": str(error), "type": type(error).__name__})
    fixture.snapshot("failure-objects")
    raise
