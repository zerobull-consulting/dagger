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
objects.** The name is generated, so it is a fresh bucket each time.

## Use

The only input is one DigitalOcean API token (a sensitive variable,
`do_token`), read from `DIGITALOCEAN_TOKEN` or `TF_VAR_do_token`. The admin key
it creates is what creates the bucket, so no Spaces key is supplied by hand.

```bash
export DIGITALOCEAN_TOKEN=...
./hack/remote-cache/spaces/spaces.sh up
```

The bucket is named `dagger-rc-test-<6 random characters>`
(`bucket_name_prefix` changes the prefix; `-var bucket_name=...` sets the whole
name).

```bash
eval "$(./hack/remote-cache/spaces/spaces.sh env write)"   # or read, or admin
export CACHESERVICE_TEST_EXTERNAL=1 CACHESERVICE_TEST_STORE=spaces CACHESERVICE_S3_PREFIX=test
```

Tear down (destroys the bucket and everything in it, then the keys):

```bash
./hack/remote-cache/spaces/spaces.sh down
```

The slug lives in the state, so `down` needs the same state directory as `up`.

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
