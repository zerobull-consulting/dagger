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

## Into GitHub

The sensitive output `gh_secret_set` is a bash script of `gh secret set` and
`gh variable set` commands that store one key's settings in a repository, or
in one of its environments. `spaces.sh github` runs it (it needs `gh`, logged
in with access to the repository's secrets):

```bash
./hack/remote-cache/spaces/spaces.sh github
# the same: terraform output -raw gh_secret_set | bash
```

| Variable | Default | What it sets |
| --- | --- | --- |
| `github_repository` | the repository `gh` finds in the current directory | `--repo owner/name` |
| `github_environment` | none: repository-level | `--env <name>` |
| `github_key` | `write` | Which key goes in: `write` or `read`. Never `admin`. |
| `github_name_prefix` | `CACHE_S3_` | The names' prefix |
| `github_all_secrets` | `false` | Store the endpoint, region, bucket and prefix as secrets too |

By default the key ID and secret become secrets and the rest variables, the
names the [remote-cache action's example](../../../.github/actions/remote-cache/README.md)
reads: secrets `CACHE_S3_ACCESS_KEY_ID` and `CACHE_S3_SECRET_ACCESS_KEY`,
variables `CACHE_S3_ENDPOINT`, `_REGION`, `_BUCKET` and `_PREFIX`. For this
repository's [Spaces tests](../../../.github/workflows/remote-cache-spaces.yml),
which read every `SPACES_*` as a secret, set
`-var github_name_prefix=SPACES_ -var github_all_secrets=true`. Each value
reaches `gh` on standard input, so no secret is on a command line. Running it
again overwrites the same names.

### A bucket per repository and environment

Each repository and environment gets its own bucket and keys from its own
state. Use a Terraform workspace each (`TF_WORKSPACE`; the first command
creates it), with the same variables for every command, best kept in a
git-ignored `.tfvars` file:

```bash
export DIGITALOCEAN_TOKEN=... TF_WORKSPACE=dagger-ci
./hack/remote-cache/spaces/spaces.sh up -var github_repository=zerobull-consulting/dagger -var github_environment=ci
./hack/remote-cache/spaces/spaces.sh github
```

In a workspace, the keys are named `dagger-rc-<workspace>-admin`, `-write`
and `-read`, so the control panel says which is which (`key_name_prefix`
overrides it). The `github_*` variables only shape the output, so changing
one is an `up` that changes no resources. To put both keys of one bucket in
GitHub, such as `write` for the default branch's environment and `read` for
pull requests', run `up` and `github` again with `-var github_key=read` and
the other environment.

## Tear down

Destroys the bucket and everything in it, then the keys:

```bash
./hack/remote-cache/spaces/spaces.sh down
```

The slug lives in the state, so `down` needs the same state directory and
workspace (`TF_WORKSPACE`) as `up`.

## Things to know

- The admin key is account-wide, and each state makes one. It stays out of
  GitHub: `github_key` refuses `admin`.
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
