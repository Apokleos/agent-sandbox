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
"""Reuse the successful main/extended test namespace for admission timing cases."""
import base64
import concurrent.futures
import json
import os
import selectors
import subprocess
from pathlib import Path
import run as fixture
import time

previous = Path(os.environ["PREVIOUS_EVIDENCE_DIR"])
original = json.loads((previous / "sandbox.json").read_text())
assert original["metadata"]["labels"]["agents.x-k8s.io/e2e-run"] == fixture.RUN
assert not fixture.kube("get", "sandboxes", "-o", "json")["items"], "run extended.py before timing.py"
assert not fixture.kube("get", "podcheckpoints", "-o", "json")["items"]
ns = fixture.kube("get", "namespace", fixture.NS, "-o", "json", namespace=False)
assert ns["metadata"]["uid"] == json.loads((previous / "namespace.json").read_text())["metadata"]["uid"]
fixture.SECRET = base64.b64decode(fixture.kube("get", "secret", "router-auth", "-o", "json")["data"]["key"]).decode()
router_ip = fixture.kube("get", "pods", "-l", "app=auto-router", "-o", "json")["items"][0]["status"]["podIP"]


def ready(value):
    return any(c["type"] == "Ready" and c["status"] == "True" and c["observedGeneration"] == value["metadata"]["generation"] for c in value.get("status", {}).get("conditions", []))


def gate_wait(name, timeout=90):
    # A status watch observes short barriers that polling can miss. The request
    # races the subsequent controller pass; either drain cancellation or a
    # completed capture+restore is legal, provided state and admission survive.
    end = time.monotonic() + timeout
    watch_command = ["--request-timeout=100s" if arg.startswith("--request-timeout=") else arg for arg in fixture.KUBE]
    watcher = subprocess.Popen(watch_command + ["-n", fixture.NS, "get", "sandbox", fixture.NAME, "--watch",
        "-o=jsonpath={.status.autoSuspension.gate}{\"\\n\"}"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    selector = selectors.DefaultSelector()
    selector.register(watcher.stdout, selectors.EVENT_READ)
    history = b""
    try:
        while time.monotonic() < end:
            for key, _ in selector.select(timeout=1):
                chunk = os.read(key.fileobj.fileno(), 4096)
                if not chunk:
                    raise RuntimeError("gate watcher ended: " + watcher.stderr.read().decode())
                history += chunk
                (fixture.ROOT / ("gate-watch-" + name + ".txt")).write_bytes(history)
                if name in history.decode().splitlines():
                    fixture.save("dispatch-" + name + ".json", {"observedGate": name, "time": time.time()})
                    return
        raise TimeoutError("admission boundary " + name)
    finally:
        watcher.terminate()
        watcher.wait(timeout=5)
        selector.close()


def posts(pool, count=4):
    return [pool.submit(fixture.request, "/increment", "POST", router_ip=router_ip) for _ in range(count)]


def collect(futures):
    fixture.wait("all queued requests completed", lambda: all(f.done() for f in futures), timeout=150)
    return [f.result() for f in futures]


try:
    spec = original["spec"]
    spec["autoSuspension"] = {"idleTimeoutSeconds": 15, "maxCycles": 2, "activityMode": "RoutedRequestsAndLeases"}
    container = spec["podTemplate"]["spec"]["containers"][0]
    container["command"] = ["python", "-u", "-c", "import time; time.sleep(6)\n" + Path(__file__).with_name("probe.py").read_text()]
    container["readinessProbe"] = {"httpGet": {"path": "/healthz", "port": 8080}, "periodSeconds": 1, "failureThreshold": 1}
    source = fixture.create({"apiVersion": "agents.x-k8s.io/v1beta1", "kind": "Sandbox", "metadata": {"name": fixture.NAME, "labels": fixture.LABELS}, "spec": spec}, "sandbox")
    before = fixture.sandbox()
    assert not ready(before)
    fixture.save("creation-before-request.json", before)
    fixture.save("creation-pods-before-request.json", fixture.pods())
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        queued = posts(pool)
        fixture.wait("creation requests registered Wake demand", lambda: any(t["metadata"].get("labels", {}).get(fixture.PREFIX + "kind") == "Wake" for t in fixture.kube("get", "leases", "-o", "json")["items"]))
        fixture.snapshot("creation-waiting")
        creation = collect(queued)
        state, _ = fixture.request()
        assert state["memory"] == state["disk"] == 4
        token = state["token"]
        assert all(response[0]["token"] == token for response in creation)
        source_pod = fixture.pods()[0]
        source_uid = source_pod["metadata"]["uid"]
        fixture.snapshot("creation-complete")

        # Hold registered work across a real Ready False->True and spec-generation
        # update, so the fixture does not depend on winning a controller race.
        background = fixture.background_acquire()
        fixture.request("/not-ready?seconds=10", "POST")
        fixture.wait("Sandbox temporarily not Ready", lambda: not ready(fixture.sandbox()))
        updated = fixture.kube("patch", "sandbox", fixture.NAME, "--type=merge", "-p", '{"spec":{"autoSuspension":{"idleTimeoutSeconds":16}}}', "-o", "json")
        assert not ready(updated)
        fixture.save("ready-update-before-request.json", updated)
        queued = posts(pool)
        fixture.snapshot("ready-update-waiting")
        refreshed = collect(queued)
        state, _ = fixture.request()
        assert state["token"] == token and state["memory"] == state["disk"] == 8
        assert fixture.pods()[0]["metadata"]["uid"] == source_uid
        assert ready(fixture.sandbox())
        fixture.snapshot("ready-update-complete")
        fixture.kube("patch", "lease", background["metadata"]["name"], "--type=json", "-p", json.dumps([
            {"op": "test", "path": "/metadata/uid", "value": background["metadata"]["uid"]},
            {"op": "replace", "path": "/metadata/labels/agents.x-k8s.io~1activity-state", "value": "Released"}]), "-o", "json")

        gate_wait("Draining")
        drained, drain_seconds = fixture.request("/increment", "POST", router_ip=router_ip)
        assert drained["token"] == token and drained["memory"] == drained["disk"] == 9
        drain_checkpoints = fixture.checkpoint_count()
        assert drain_checkpoints in (0, 1)
        if drain_checkpoints == 0:
            assert fixture.pods()[0]["metadata"]["uid"] == source_uid
        source_uid = fixture.pods()[0]["metadata"]["uid"]
        fixture.snapshot("drain-request-complete")

        gate_wait("Checkpointing")
        queued = posts(pool)
        fixture.snapshot("checkpoint-request-waiting")
        captured = collect(queued)
        state, _ = fixture.request()
        assert state["token"] == token and state["memory"] == state["disk"] == 13
        restored = fixture.pods()
        assert len(restored) == 1 and restored[0]["metadata"]["uid"] != source_uid
        checkpoint = fixture.kube("get", "podcheckpoints", "-o", "json")["items"]
        assert len(checkpoint) == drain_checkpoints + 1
        active = [cp for cp in checkpoint if cp["metadata"]["name"] == restored[0]["spec"]["restoreFrom"]["name"]]
        assert len(active) == 1 and active[0]["spec"]["sourcePod"]["uid"] == source_uid
        assert any(ref.get("controller") and ref["uid"] == source["metadata"]["uid"] for ref in restored[0]["metadata"]["ownerReferences"])
        fixture.snapshot("checkpoint-request-complete")
        fixture.save("result.json", {"result": "PASS", "runtimeClass": fixture.RUNTIME,
            "cases": ["requests-during-creation-before-Ready", "requests-during-Ready-generation-update", "request-during-Draining", "requests-during-Checkpointing"],
            "creationRequestSeconds": [r[1] for r in creation], "readyUpdateRequestSeconds": [r[1] for r in refreshed],
            "drainRequestSeconds": drain_seconds, "drainCheckpointCount": drain_checkpoints,
            "checkpointRequestSeconds": [r[1] for r in captured], "finalState": state})
    fixture.kube("patch", "sandbox", fixture.NAME, "--type=merge", "-p", '{"spec":{"operatingMode":"Suspended"}}', "-o", "json")
    fixture.wait("timing workload stopped", lambda: not fixture.pods())
    print("PASS: creation, Ready update and hibernation admission boundaries", flush=True)
except BaseException as error:
    fixture.save("failure.json", {"type": type(error).__name__, "error": str(error)})
    fixture.snapshot("failure-objects")
    raise
