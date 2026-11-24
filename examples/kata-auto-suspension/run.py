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
"""Run real Router-driven chained Kata C/R; preserve evidence and stop on failure.

Requires a preflighted dedicated test window. Cleanup is deliberately separate
so a failure never destroys the objects needed for diagnosis.
"""
import base64
import concurrent.futures
import hashlib
import hmac
import json
import os
import re
from pathlib import Path
import subprocess
import time
import urllib.error
import urllib.request
import uuid

NS = os.environ["E2E_NAMESPACE"]
RUN = os.environ["E2E_RUN_ID"]
RUNTIME = os.environ["RUNTIME_CLASS"]
ROOT = Path(os.environ["EVIDENCE_DIR"])
ROOT.mkdir(exist_ok=False)
CONTEXT = os.environ["KUBE_CONTEXT"]
ROUTER_IMAGE = os.environ["ROUTER_IMAGE"]
PROBE_IMAGE = os.environ.get("PROBE_IMAGE", "m.daocloud.io/docker.io/library/python:3.12-alpine")
LABELS = {"agents.x-k8s.io/e2e-run": RUN, "agents.x-k8s.io/e2e-suite": "agent-sandbox-pod-cr"}
PREFIX = "agents.x-k8s.io/activity-"
NAME = "continuity"
SECRET = os.urandom(32).hex()
KUBE = ["kubectl", "--context", CONTEXT, "--request-timeout=20s"]
events = []
known_cri = set()
known_processes = {}


def save(name, data):
    (ROOT / name).write_text(json.dumps(data, indent=2))


def kube(*args, obj=None, namespace=True):
    command = KUBE + (["-n", NS] if namespace else []) + list(args)
    result = subprocess.run(command, input=json.dumps(obj) if obj else None,
                            text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode:
        raise RuntimeError(f"kubectl {' '.join(args)}: {result.stderr}")
    return json.loads(result.stdout) if result.stdout.strip() else None


def create(obj, name=None, namespace=True):
    result = kube("create", "-f", "-", "-o", "json", obj=obj, namespace=namespace)
    if name:
        save(name + ".json", result)
    return result


def sandbox():
    value = kube("get", "sandbox", NAME, "-o", "json")
    for cond in value.get("status", {}).get("conditions", []):
        if cond.get("reason") in {"ReconcilerError", "CheckpointFailed", "RestorePodFailed", "RestoreTimedOut", "CheckpointTimedOut"}:
            raise RuntimeError(f"terminal condition: {cond}")
    return value


def wait(description, predicate, timeout=180):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        value = predicate()
        if value:
            events.append({"event": description, "time": time.time()})
            save("timeline.json", events)
            print(description, flush=True)
            return value
        time.sleep(1)
    raise TimeoutError(description)


def gate(name):
    value = sandbox()
    return value if value.get("status", {}).get("autoSuspension", {}).get("gate") == name else None


def pods():
    return kube("get", "pods", "-l", "app=auto-probe", "-o", "json")["items"]


def checkpoint_count():
    return len(kube("get", "podcheckpoints", "-o", "json")["items"])


def snapshot(name):
    value = kube("get", "sandbox,pod,podcheckpoint,lease,events", "-o", "json")
    save(name + ".json", value)
    host_sample(name)
    routers = kube("get", "pods", "-l", "app=auto-router", "-o", "json")["items"]
    ready = [p for p in routers if not p["metadata"].get("deletionTimestamp") and p.get("status", {}).get("podIP")]
    if len(ready) == 1:
        with urllib.request.urlopen("http://" + ready[0]["status"]["podIP"] + ":9090/metrics", timeout=5) as metrics:
            (ROOT / (name + "-router-metrics.txt")).write_text(metrics.read().decode())
        save(name + "-router-identity.json", {"uid": ready[0]["metadata"]["uid"], "imageID": ready[0]["status"]["containerStatuses"][0]["imageID"]})
    return value


def host_sample(name):
    """Optional node-side measurements; never infer RSS from Pod requests."""
    if os.environ.get("NODE_EVIDENCE") != "1":
        return
    cri = json.loads(subprocess.check_output(["sudo", "-n", "crictl", "--runtime-endpoint", "unix:///run/containerd/containerd.sock", "pods", "-o", "json"], text=True))
    matches = [item for item in cri.get("items", []) if item["metadata"].get("namespace") == NS]
    known_cri.update(item["id"] for item in matches)
    ps = subprocess.check_output(["ps", "-eo", "pid=,ppid=,rss=,time=,args="], text=True)
    processes = []
    for line in ps.splitlines():
        fields = line.split(None, 4)
        if len(fields) == 5:
            processes.append({"pid": int(fields[0]), "ppid": int(fields[1]), "rssKiB": int(fields[2]), "cpuTime": fields[3], "args": fields[4]})
    def start_ticks(pid):
        try:
            return Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()[19]
        except FileNotFoundError:
            return None
    selected = {p["pid"] for p in processes if "containerd-shim-kata" in p["args"] and any(identifier in p["args"] for identifier in known_cri)}
    # Keep identities after reparenting: QEMU may outlive its shim temporarily.
    selected |= {p["pid"] for p in processes if p["pid"] in known_processes and start_ticks(p["pid"]) == known_processes[p["pid"]]}
    while True:
        children = {p["pid"] for p in processes if p["ppid"] in selected}
        if children <= selected:
            break
        selected |= children
    owned = [p for p in processes if p["pid"] in selected]
    for process in owned:
        ticks = start_ticks(process["pid"])
        if ticks is not None:
            known_processes[process["pid"]] = ticks
    inspect = {}
    inspection_errors = {}
    for sandbox in matches:
        result = subprocess.run(["sudo", "-n", "crictl", "--runtime-endpoint", "unix:///run/containerd/containerd.sock", "inspectp", sandbox["id"]], text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if result.returncode:
            if "code = NotFound" not in result.stderr:
                raise RuntimeError(result.stderr)
            inspection_errors[sandbox["id"]] = result.stderr
        else:
            inspect[sandbox["id"]] = json.loads(result.stdout)
    assets = []
    for checkpoint in kube("get", "podcheckpoints", "-o", "json")["items"]:
        location = checkpoint.get("status", {}).get("checkpointLocation", {}).get("nodeLocal", {}).get("path", "")
        if re.fullmatch(r"checkpoint-[a-f0-9-]+", location):
            path = "/var/lib/kubelet/pod-checkpoints/" + location
            result = subprocess.run(["sudo", "-n", "du", "-sb", path], text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            assets.append({"checkpointUID": checkpoint["metadata"]["uid"], "path": path, "du": result.stdout.strip(), "error": result.stderr.strip()})
    result = {"time": time.time(), "cri": matches, "inspect": inspect, "inspectionErrors": inspection_errors, "knownCRI": sorted(known_cri), "knownProcessIdentities": known_processes, "processes": owned, "totalRssKiB": sum(p["rssKiB"] for p in owned), "assets": assets}
    save(name + "-node.json", result)
    return result


def request(path="/state", method="GET", target_ns=NS, expect=200, router_ip=None):
    if router_ip is None:
        routers = kube("get", "pods", "-l", "app=auto-router", "-o", "json")["items"]
        ready = [p for p in routers if not p["metadata"].get("deletionTimestamp") and p.get("status", {}).get("podIP")]
        assert len(ready) == 1, routers
        router_ip = ready[0]["status"]["podIP"]
    payload = base64.urlsafe_b64encode(json.dumps({"ns": NS, "name": NAME, "exp": int(time.time()) + 1800}, separators=(",", ":")).encode()).rstrip(b"=")
    sig = base64.urlsafe_b64encode(hmac.new(SECRET.encode(), b"agent-sandbox/scoped-token/v1." + payload, hashlib.sha256).digest()).rstrip(b"=")
    token = b"v1." + payload + b"." + sig
    req = urllib.request.Request("http://" + router_ip + ":8080" + path,
        data=b"{}" if method == "POST" else None,
        headers={"Authorization": "Bearer " + token.decode(), "X-Sandbox-Id": NAME,
                 "X-Sandbox-Namespace": target_ns, "X-Sandbox-Port": "8080"}, method=method)
    start = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=150) as response:
            status, body = response.status, response.read()
    except urllib.error.HTTPError as error:
        status, body = error.code, error.read()
    assert status == expect, (status, body.decode())
    value = json.loads(body)
    return value, time.monotonic() - start


def no_checkpoint(seconds):
    end = time.monotonic() + seconds
    initial = checkpoint_count()
    while time.monotonic() < end:
        sandbox()
        assert checkpoint_count() == initial, "checkpoint while protected by activity"
        assert len(pods()) == 1
        time.sleep(1)


def background_acquire():
    before = wait("Open for background admission", lambda: gate("Open"))
    state = before["status"]["autoSuspension"]
    ticket = create({"apiVersion": "coordination.k8s.io/v1", "kind": "Lease",
        "metadata": {"name": "as-activity-" + str(uuid.uuid4()), "labels": {
            **LABELS, PREFIX + "sandbox-uid": before["metadata"]["uid"], PREFIX + "kind": "Background",
            PREFIX + "state": "Active", PREFIX + "epoch": str(state["epoch"])},
            "ownerReferences": [{"apiVersion": "agents.x-k8s.io/v1beta1", "kind": "Sandbox", "name": NAME,
                                 "uid": before["metadata"]["uid"], "controller": True}]},
        "spec": {"holderIdentity": "e2e-background", "renewTime": "2000-01-01T00:00:00.000000Z", "leaseDurationSeconds": 1}}, "background-ticket")
    after = sandbox()
    assert after["metadata"]["uid"] == before["metadata"]["uid"]
    assert after["status"]["autoSuspension"]["gate"] == "Open"
    assert after["status"]["autoSuspension"]["epoch"] == state["epoch"]
    return ticket


def run():
    create({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": NS, "labels": LABELS}}, "namespace", namespace=False)
    create({"apiVersion": "v1", "kind": "ResourceQuota", "metadata": {"name": "activity-budget"},
            "spec": {"hard": {"count/leases.coordination.k8s.io": "256"}}})
    create({"apiVersion": "v1", "kind": "ServiceAccount", "metadata": {"name": "auto-router"}})
    create({"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role", "metadata": {"name": "auto-router"}, "rules": [
        {"apiGroups": ["agents.x-k8s.io"], "resources": ["sandboxes"], "verbs": ["get"]},
        {"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list"]},
        {"apiGroups": ["coordination.k8s.io"], "resources": ["leases"], "verbs": ["get", "create", "update", "patch", "delete"]}]})
    create({"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding", "metadata": {"name": "auto-router"},
        "subjects": [{"kind": "ServiceAccount", "name": "auto-router", "namespace": NS}],
        "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": "auto-router"}})
    create({"apiVersion": "v1", "kind": "Secret", "metadata": {"name": "router-auth"}, "stringData": {"key": SECRET}})
    create({"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": "auto-router", "labels": LABELS},
        "spec": {"replicas": 1, "selector": {"matchLabels": {"app": "auto-router"}}, "template": {
            "metadata": {"labels": {**LABELS, "app": "auto-router"}}, "spec": {"serviceAccountName": "auto-router",
            "containers": [{"name": "router", "image": ROUTER_IMAGE, "imagePullPolicy": "Never", "command": ["/router"],
                "args": ["--enable-auto-suspension", "--cache-enabled=false", "--authz-mode=scoped-token",
                         "--authz-scoped-token-secret-file=/auth/key", "--proxy-timeout=120s"],
                "securityContext": {"runAsUser": 65532, "runAsGroup": 65532},
                "volumeMounts": [{"name": "auth", "mountPath": "/auth", "readOnly": True}],
                "readinessProbe": {"httpGet": {"path": "/healthz", "port": 8080}}}],
            "volumes": [{"name": "auth", "secret": {"secretName": "router-auth"}}]}}}}, "router-deployment")
    subprocess.run(KUBE + ["-n", NS, "rollout", "status", "deployment/auto-router", "--timeout=90s"], check=True)
    source = create({"apiVersion": "agents.x-k8s.io/v1beta1", "kind": "Sandbox", "metadata": {"name": NAME, "labels": LABELS},
        "spec": {"operatingMode": "Running", "autoSuspension": {"idleTimeoutSeconds": 8, "maxCycles": 2, "activityMode": "RoutedRequestsAndLeases"},
        "podTemplate": {"metadata": {"labels": {**LABELS, "app": "auto-probe"}}, "spec": {
            "runtimeClassName": RUNTIME, "automountServiceAccountToken": False, "hostNetwork": False,
            "containers": [{"name": "probe", "image": PROBE_IMAGE, "imagePullPolicy": "IfNotPresent",
                "command": ["python", "-u", "-c", (Path(__file__).with_name("probe.py")).read_text()],
                "readinessProbe": {"tcpSocket": {"port": 8080}, "periodSeconds": 1}}]}}}}, "sandbox")
    wait("source Open and Ready", lambda: gate("Open") and any(c["type"] == "Ready" and c["status"] == "True" for c in sandbox()["status"]["conditions"]))
    initial, duration = request("/increment", "POST")
    save("initial-state.json", initial)
    source_pod = pods()[0]
    save("source-pod.json", source_pod)
    host_sample("source")
    request(target_ns="default", expect=403)
    assert checkpoint_count() == 0
    ticket = background_acquire()
    no_checkpoint(12)
    # Exercise an expected API failure without admitting or replaying work.
    assert len(kube("get", "leases", "-o", "json")["items"]) == 1
    kube("patch", "resourcequota", "activity-budget", "--type=merge", "-p", '{"spec":{"hard":{"count/leases.coordination.k8s.io":"1"}}}', "-o", "json")
    wait("Lease quota tightened", lambda: kube("get", "resourcequota", "activity-budget", "-o", "json").get("status", {}).get("hard", {}).get("count/leases.coordination.k8s.io") == "1")
    request(expect=503)
    assert len(kube("get", "leases", "-o", "json")["items"]) == 1
    kube("patch", "resourcequota", "activity-budget", "--type=merge", "-p", '{"spec":{"hard":{"count/leases.coordination.k8s.io":"256"}}}', "-o", "json")
    wait("Lease quota restored", lambda: kube("get", "resourcequota", "activity-budget", "-o", "json").get("status", {}).get("hard", {}).get("count/leases.coordination.k8s.io") == "256")
    old_router = kube("get", "pods", "-l", "app=auto-router", "-o", "json")["items"][0]
    subprocess.run(KUBE + ["-n", NS, "delete", "pod", old_router["metadata"]["name"], "--wait=true", "--timeout=60s"], check=True)
    subprocess.run(KUBE + ["-n", NS, "rollout", "status", "deployment/auto-router", "--timeout=90s"], check=True)
    request()
    no_checkpoint(3)
    kube("patch", "lease", ticket["metadata"]["name"], "--type=json", "-p", json.dumps([
        {"op": "test", "path": "/metadata/uid", "value": ticket["metadata"]["uid"]},
        {"op": "replace", "path": "/metadata/labels/agents.x-k8s.io~1activity-state", "value": "Released"}]), "-o", "json")
    with concurrent.futures.ThreadPoolExecutor() as pool:
        pending = pool.submit(request, "/hold?seconds=14")
        wait("long request has Active ticket", lambda: any(t["metadata"].get("labels", {}).get(PREFIX + "kind") == "Request" and t["metadata"].get("labels", {}).get(PREFIX + "state") == "Active" for t in kube("get", "leases", "-o", "json")["items"]))
        no_checkpoint(10)
        pending.result()
    snapshot("activity-protection")
    previous = initial
    previous_uid = source_pod["metadata"]["uid"]
    results = []
    for cycle in (1, 2):
        sleep_start = time.monotonic()
        wait(f"automatic hibernation {cycle}", lambda: gate("Hibernated"))
        idle_to_hibernated = time.monotonic() - sleep_start
        assert not pods(), "source Pod still present"
        state = sandbox()
        assert state["spec"]["operatingMode"] == "Running" and state["metadata"]["generation"] == source["metadata"]["generation"]
        checkpoint = kube("get", "podcheckpoint", state["status"]["checkpointRestore"]["checkpointName"], "-o", "json")
        save(f"checkpoint-{cycle}.json", checkpoint)
        assert checkpoint["spec"]["sourcePod"]["uid"] == previous_uid
        snapshot(f"hibernated-{cycle}")
        if os.environ.get("NODE_EVIDENCE") == "1":
            # kubelet removes stopped sandboxes on its 60s container GC loop.
            # Keep the process-exit assertion; allow one normal cycle + margin.
            wait(f"hibernation {cycle} released tracked runtime processes", lambda: not host_sample(f"hibernated-{cycle}-settled")["processes"], timeout=90)
        if cycle == 1:
            controllers = kube("get", "pods", "-n", "agent-sandbox-system", "-l", "app=agent-sandbox-controller", "-o", "json")["items"]
            old = controllers[0]
            subprocess.run(KUBE + ["-n", "agent-sandbox-system", "delete", "pod", old["metadata"]["name"], "--wait=false"], check=True)
            subprocess.run(KUBE + ["-n", "agent-sandbox-system", "rollout", "status", "deployment/agent-sandbox-controller", "--timeout=90s"], check=True)
            assert gate("Hibernated"), "restart unexpectedly reopened gate"
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            responses = list(pool.map(lambda _: request("/increment", "POST"), range(4)))
        current = pods()
        assert len(current) == 1
        restored = current[0]
        assert restored["metadata"]["uid"] != previous_uid
        assert restored["spec"]["restoreFrom"]["name"] == checkpoint["metadata"]["name"]
        states = [response[0] for response in responses]
        assert all(s["token"] == initial["token"] for s in states), states
        final, _ = request()
        assert final["memory"] == previous["memory"] + 4 and final["disk"] == previous["disk"] + 4, final
        previous, previous_uid = final, restored["metadata"]["uid"]
        assert checkpoint_count() == cycle
        dns = subprocess.check_output(KUBE + ["-n", NS, "exec", NAME, "-c", "probe", "--", "python", "-c",
            "import socket; print(socket.getaddrinfo('kubernetes.default.svc.cluster.local',443))"], text=True)
        (ROOT / f"dns-{cycle}.txt").write_text(dns)
        snapshot(f"restored-{cycle}")
        results.append({"cycle": cycle, "request_seconds": [r[1] for r in responses], "idleToHibernatedSeconds": idle_to_hibernated, "wholeCycleSeconds": time.monotonic() - sleep_start, "state": final})
    no_checkpoint(13)
    assert sandbox()["status"]["autoSuspension"]["cycles"] == 2
    kube("patch", "sandbox", NAME, "--type=merge", "-p", '{"spec":{"operatingMode":"Suspended"}}', "-o", "json")
    wait("manual Suspended has no Pod", lambda: not pods())
    request(expect=409)
    assert not pods(), "traffic resumed manual suspension"
    snapshot("manual-suspended")
    save("result.json", {"result": "PASS", "runtimeClass": RUNTIME, "cases": ["background", "no-ticket-expiry", "lease-quota-failure", "long-request", "authorization", "chained-auto-cr", "concurrent-wake", "controller-restart", "router-restart", "cycle-budget", "manual-suspended"], "cycles": results})
    print("PASS: Router-driven two-cycle auto C/R", flush=True)


if __name__ == "__main__":
    try:
        run()
    except BaseException as error:
        save("failure.json", {"type": type(error).__name__, "error": str(error)})
        try:
            snapshot("failure-objects")
        except BaseException:
            pass
        raise
