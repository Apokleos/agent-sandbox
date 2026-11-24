# Kata Pod checkpoint and restore

This example runs two consecutive hibernate and restore cycles against an already deployed candidate. It checks the Sandbox owner and the restored Pod's checkpoint reference, then samples an in-memory counter, a boot token and a writable-rootfs counter. It retains the test namespace and evidence for inspection.

Use a dedicated Kubernetes test cluster with the PodCheckpoint API, Pod restore admission and a matching containerd and Kata QEMU build. Deploy the candidate Agent Sandbox Controller with `--enable-pod-checkpoint-restore` and an explicit `--pod-checkpoint-runtime-classes` allowlist. Verify that the selected RuntimeClass uses the C/R profile and that the active controller, runtime and guest assets match the recorded builds.

```bash
KUBE_CONTEXT=kata-cr-lab RUNTIME_CLASS=kata-pod-ckpt \
  E2E_NAMESPACE=as-cr-e2e-demo E2E_RUN_ID=demo \
  EVIDENCE_DIR=./kata-cr-evidence bash ./run.sh
```

The runner requires Bash, kubectl and jq. Reserve a new namespace and run ID before preflight and use the same values here. The namespace must be unused. Use a new evidence directory for each invocation. Set `PROBE_IMAGE` to the test image by digest when archiving reproducible evidence. An installed upstream distribution may lack the experimental lower-layer APIs. Record the exact Git refs and patches, artifacts, runtime configuration, and candidate Controller digest used to set up the test cluster.

The output covers the Kubernetes-level success path. A full E2E result additionally requires controller and node-level logs, CNI/DNS and application reachability proof, runtime task continuity, restart and failure injection, and API plus node-level cleanup. The runner never reports that full result. It prints the run namespace and leaves cleanup to the operator after evidence has been captured.
