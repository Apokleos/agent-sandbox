# Experimental automatic hibernation admission

The Go Router can hold an authorized HTTP request while an opt-in Kata Sandbox
restores, then forward it to the current Ready Pod. Enable
`--enable-auto-suspension --authz-mode=scoped-token` and provide the existing
`--authz-scoped-token-secret-file`. The feature is disabled by default.
TokenReview alone authenticates callers without restricting their target
Sandbox, so this experiment requires the scoped-token authorizer.

All traffic for an opted-in Sandbox must pass through an enabled Router. Mixing
enabled and legacy Router replicas, bypassing the Router, or adding the policy
to a workload with untracked work does not provide the activity guarantee.
The caller's PodIP and UID override headers remain forbidden by scoped-token
authorization. After admission the Router obtains the Ready Pod directly from
the Kubernetes API and does not use a stale Pod cache entry.

Each admitted request holds a Kubernetes Lease until the upstream request or
upgraded stream ends. The controller closes the Sandbox gate before listing
active tickets and checkpointing. An admission requires an Open gate, creation
of a ticket, and a second read of the same Open epoch. Application requests are
forwarded once; automatic retry and upstream connection reuse are disabled on
this path. This costs additional API requests and upstream connections.

The Router needs namespace-scoped `get` on Sandboxes, `list` on Pods, and
`create/get/update/delete` on Leases. Deletion is used only for wake demands;
completed request tickets are marked Released for controller consumption.
Configure a namespace ResourceQuota for `count/leases.coordination.k8s.io` so
an unavailable controller cannot accumulate unlimited completion records.
The Router rejects admission when ticket creation fails, including quota
exhaustion, and limits active plus waiting requests to 64 per instance.

## Background work

HTTP completion does not prove the application's background work has ended.
Tasks that continue after an HTTP disconnect must acquire their own activity
ticket **before** starting work. The internal Go helper uses the same handshake:

```go
client := activity.New(dynamicClient, kubernetesClient, activity.Options{
    Identity: "trusted-worker-instance-id",
})
permit, err := client.Acquire(ctx, namespace, sandboxName, activity.Background)
if err != nil {
    return err // Do not start the task.
}
if permit == nil {
    return fmt.Errorf("background activity policy is not enabled")
}
// Run the task to completion. Releasing after HTTP disconnect is too early
// if the task is still running.
err = runTask()
cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
releaseErr := permit.Release(cleanup)
return errors.Join(err, releaseErr)
```

This helper is internal to the repository, not a published SDK. Trusted clients
implementing the wire protocol use Lease labels
`agents.x-k8s.io/activity-sandbox-uid`, `activity-kind`, `activity-state`, and
`activity-epoch` (all under the same `agents.x-k8s.io/` prefix), plus an exact
Sandbox controller OwnerReference. Kinds are Request, Wake, Background; states
are Active and Released. Client timestamps are audit data, not proof of idleness.

Leases have no automatic activity expiry. A crashed client or failed completion
write leaves protection in place and blocks automatic hibernation. Before
manually removing such a ticket, prove the associated task has ended; Router
process death alone is insufficient if the application continued running.
User-driven manual suspension and absolute expiration still take precedence
and can terminate running work.

When enabled, `/metrics` includes
`sandbox_router_activity_api_requests_total{operation,result}`. The six fixed
operations are SandboxGet, LeaseCreate, LeaseGet, LeaseUpdate, LeaseDelete and
PodList; result is success or error. These count logical Router helper calls,
including the policy read for legacy Sandboxes, and reset on Router restart.
Use Pod identities and counter deltas. They do not count controller traffic or
client-go's internal HTTP retries and are not total control-plane API QPS.
