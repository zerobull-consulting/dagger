# Spaces test bucket (Terraform)

Creates a dedicated DigitalOcean Spaces bucket and the three keys the remote
cache service documents ([The keys](../../../cmd/cacheservice/README.md#the-keys)),
for the tests of [Testing against a real bucket](../README.md#testing-against-a-real-bucket),
and removes them again.

| Key | Spaces grant | Use |
| --- | --- | --- |
| `-admin` | full access (account-wide: Spaces can't scope it to a bucket) | `cacheservice lifecycle apply`, interactively. Never in CI. |
| `-write` | `readwrite` on the bucket | The writer service, export-capable jobs, GC. |
| `-read` | `read` on the bucket | Pull-request jobs, `-export=false`. Can't write. |

The bucket is private and has `force_destroy`: **tear-down deletes its
objects.** Use a name nobody else's data lives under.

## Use

You need a DigitalOcean API token and a Spaces key that can create buckets,
in the environment (the provider reads these names):

```bash
export DIGITALOCEAN_TOKEN=... SPACES_ACCESS_KEY_ID=... SPACES_SECRET_ACCESS_KEY=...
```

```bash
./hack/remote-cache/spaces/spaces.sh up -var bucket_name=dagger-rc-test-<you>
```

```bash
eval "$(./hack/remote-cache/spaces/spaces.sh env write)"   # or read, or admin
export CACHESERVICE_TEST_EXTERNAL=1 CACHESERVICE_TEST_STORE=spaces CACHESERVICE_S3_PREFIX=test
```

Tear down (destroys the keys, then the bucket and everything in it):

```bash
./hack/remote-cache/spaces/spaces.sh down -var bucket_name=dagger-rc-test-<you>
```

Pass the same `-var` flags to `down` as to `up`, or put them in a
`terraform.tfvars` (git-ignored).

## Things to know

- Terraform state holds the keys' secrets. It is local and git-ignored; treat
  the directory as sensitive, and run `down` before discarding it. Losing the
  state leaves the bucket and keys behind, to delete in the control panel.
- Spaces keys are scoped to a bucket, not a prefix, so `prefix` only shapes
  the outputs. The service and tests keep to their prefix themselves.
- This was written without a `terraform` binary or DigitalOcean account to
  hand, so it hasn't been applied. Run `terraform validate` and a first `up`
  against a throwaway name before relying on it. Check that a `-write` key can
  read lifecycle rules (the README marks this as not yet run) and that `-read`
  cannot write.
