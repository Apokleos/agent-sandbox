# Experimental Kata auto suspension

This example extends the opt-in PodCheckpoint provider with Router-driven
hibernation and wake-up. It requires the experimental Kubernetes/containerd
PodCheckpoint and restoreFrom APIs; an ordinary Kubernetes installation is
insufficient. This is a local prototype, not an accepted upstream API.

Enable the controller's existing Pod checkpoint feature and allow the exact
RuntimeClass. Create a new Sandbox with:

```yaml
spec:
  operatingMode: Running
  autoSuspension:
    idleTimeoutSeconds: 60
    maxCycles: 2
    activityMode: RoutedRequestsAndLeases
```

Configure policy at creation. Adding it to a previously running Sandbox is
rejected, because existing requests may not have activity tickets. Removal
is supported; the controller finishes an active transition and restores safely.
The lifetime cycle budget is retained. Re-enabling policy requires a new Sandbox.

Run the Go sandbox-router with `--enable-auto-suspension`, scoped-token
authorization and a mounted signing secret of at least 32 bytes. Ordinary
unauthenticated routing and the existing authentication-only TokenReview mode
cannot enable this feature. Authorize each client for its Sandbox. The Router
ServiceAccount needs read access to Sandboxes and Pods, and create/get/update/
patch/delete access to activity Leases in the intended namespace. Configure a
namespace ResourceQuota for `count/leases.coordination.k8s.io` (the runner uses
256). Requests fail closed when the API or quota prevents admission.

The controller keeps user `operatingMode: Running`. Its allocated gate closes
before checkpointing. Requests register a durable Lease and recheck the same
Open gate and epoch before forwarding. The Router keeps the first wake request
while a restored Pod becomes Ready, then forwards once using its current IP.
Application requests are not automatically retried. Long HTTP requests, SSE and
WebSocket connections retain their ticket until the proxy returns.

All workload traffic must use the enabled Router. Background work, including
work that outlives a disconnected request, must acquire a Background permit via
the same `internal/activity` Open/create/recheck protocol before it begins.
Release after work actually ends. A bare Lease create without the second read
is unsafe. Leases never expire automatically: an unknown/orphan Active ticket
blocks hibernation until an operator proves both its Router and application work
have terminated, then removes that exact ticket. Avoid shared cluster-wide
credentials in workload containers.

Only the Phase 1 supported runtime profile is accepted: CNI networking, no
volumes/devices/extended resources, automountServiceAccountToken=false, and the
explicitly allowed Kata runtime. Checkpoints are node-local and restore on the
same node. No node scale-down, migration, process-only freeze, transparent
arbitrary TCP reconnection or retention collector is provided. `maxCycles`
bounds the number of automatic attempts, including failed attempts; it is not
a byte quota. Retained assets remain an operator responsibility.

For the dedicated test window, build and deploy the candidate controller and
Router, verify the active image IDs, apply generated schema/RBAC with a saved
inverse patch, then run:

```sh
KUBE_CONTEXT=<context> RUNTIME_CLASS=kata-cr-dragonball-isolated \
E2E_NAMESPACE=<unused-namespace> E2E_RUN_ID=<unique-run> \
EVIDENCE_DIR=<new-directory> ROUTER_IMAGE=<imported-image> \
python3 examples/kata-auto-suspension/run.py
```

Repeat independently with `kata-cr-qemu-review-isolated`. The runner creates
the isolated namespace, minimal Router RBAC, quota, scoped authentication and
the no-volume stateful probe. It validates background and long-request
protection, authorization, two automatic C/R cycles with memory/rootfs/boot
continuity, concurrent wake, controller restart, cycle budget and manual
Suspended precedence. A failure preserves objects and evidence for diagnosis;
the dedicated-window owner must clean up by recorded UID and restore the
original controller, schema and RBAC. These cases do not replace the full
Phase 1 AS-A through AS-G suite.

After `extended.py` has removed its Sandboxes, retain the same variables and
`PREVIOUS_EVIDENCE_DIR`, choose another new `EVIDENCE_DIR`, and run
`python3 examples/kata-auto-suspension/timing.py`. It sends concurrent requests
while a new workload is still Pending/not Ready, then during a real readiness
transition with an updated generation. A status watch dispatches requests at
Draining and Checkpointing boundaries. A drain race may either cancel the drain
or finish capture/restore; both must preserve state and forward requests once.
The fixture delays initial startup and briefly toggles its own readiness; no
controller timing hooks are installed. It leaves its recorded Sandbox manually
Suspended so the dedicated-window owner can delete it by UID during rollback.

After success, keep the same namespace, run identity, context, RuntimeClass and
Router image variables, point `PREVIOUS_EVIDENCE_DIR` to that runner directory,
set `EVIDENCE_DIR` to a new directory, then execute
`python3 examples/kata-auto-suspension/extended.py`. It safely deletes the
recorded primary Sandbox by UID and uses new Sandbox identities to test policy
removal during checkpoint, rejection of live policy enable/re-enable, and
AlwaysRunning/cold-recreate baselines. It preserves failure evidence and also
requires the dedicated-window rollback procedure afterward.

Set `NODE_EVIDENCE=1` only on the test node with read-only sudo access to crictl
and checkpoint directories. This captures historical PID/starttime identities,
RSS, CPU time and snapshot sizes. It waits up to 90 seconds for tracked runtime
processes to disappear before wake: kubelet's normal stopped-sandbox GC can take
one minute. No production delay, forced GC or process termination is introduced.

Measure host VM/shim RSS and CPU, checkpoint bytes and elapsed times, wake
request latency, request failures and API overhead before claiming savings.
Compare equivalent AlwaysRunning and cold-start workloads and state the sample
count. State continuity alone proves functionality, not a latency improvement.
