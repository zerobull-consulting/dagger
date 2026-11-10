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

Two sensitive outputs, `gh_secret_set_write` and `gh_secret_set_read`, are
bash scripts of `gh secret set` and `gh variable set` commands. Each stores
one key's settings in a repository, or in one of its environments, so the
write key and the read key never share a place. The admin key has no
script: no job needs it. Run them with `spaces.sh github` (it needs `gh`,
logged in with access to the repository's secrets):

```bash
./hack/remote-cache/spaces/spaces.sh github write
./hack/remote-cache/spaces/spaces.sh github read
# the same: terraform -chdir=hack/remote-cache/spaces output -raw gh_secret_set_write | bash
```

| Variable | Default | What it sets |
| --- | --- | --- |
| `github_repository` | the repository `gh` finds in the current directory | `--repo owner/name`, for both |
| `github_write_environment` | none: repository-level | the write key's `--env` |
| `github_read_environment` | none: repository-level | the read key's `--env` |
| `github_write_name_prefix` | `CACHE_S3_` | the write key's names |
| `github_read_name_prefix` | `CACHE_S3_READ_` | the read key's names |
| `github_all_secrets` | `false` | store the endpoint, region, bucket and prefix as secrets too |

Each script sets `<prefix>ACCESS_KEY_ID` and `<prefix>SECRET_ACCESS_KEY` as
secrets, and `<prefix>ENDPOINT`, `_REGION`, `_BUCKET` and `_PREFIX` as
variables (as secrets with `github_all_secrets`), the names the
[remote-cache action's example](../../../.github/actions/remote-cache/README.md)
reads with the `CACHE_S3_` prefix. Each value reaches `gh` on standard input,
so no secret is on a command line. Running a script again overwrites the
same names.

There are two ways to keep the keys apart:

- **Environments** (recommended): the write key in the environment of the
  jobs that write, such as the default branch's, and the read key in pull
  requests'. Both can then use the `CACHE_S3_` names, so a job's steps are
  the same whichever key it gets:

  ```bash
  ./hack/remote-cache/spaces/spaces.sh up \
    -var github_repository=zerobull-consulting/dagger \
    -var github_write_environment=main \
    -var github_read_environment=pull-requests \
    -var github_read_name_prefix=CACHE_S3_
  ```

  Restrict who can use the write key's environment in the repository's
  settings (deployment branches: the default branch only), or any branch can
  pick it.
- **Names**, both repository-level (the defaults): the write key as
  `CACHE_S3_*` and the read key as `CACHE_S3_READ_*`. Simpler, but every
  workflow on every branch can read both.

`up` refuses (a failed precondition, before it creates anything) a
combination that would store both keys under the same names in the same
place. For this repository's
[Spaces tests](../../../.github/workflows/remote-cache-spaces.yml), which
read every `SPACES_*` as a secret, set `-var github_write_name_prefix=SPACES_
-var github_all_secrets=true` and run `github write`.

The `github_*` variables only shape the outputs, so changing one is an `up`
that changes no resources.

### A bucket per repository

Each repository gets its own bucket and keys from its own state. Use a
Terraform workspace each (`TF_WORKSPACE`; the first command creates it),
with the same variables for every command, best kept in a git-ignored
`.tfvars` file:

```bash
export DIGITALOCEAN_TOKEN=... TF_WORKSPACE=dagger
./hack/remote-cache/spaces/spaces.sh up -var-file=dagger.tfvars
./hack/remote-cache/spaces/spaces.sh github write
./hack/remote-cache/spaces/spaces.sh github read
```

In a workspace, the keys are named `dagger-rc-<workspace>-admin`, `-write`
and `-read`, so the control panel says which is which (`key_name_prefix`
overrides it).

## Lifecycle rules

`spaces.sh lifecycle` runs [`cacheservice lifecycle`](../../../cmd/cacheservice/README.md#the-lifecycle-command)
with the admin key, the only one Spaces lets read or write lifecycle rules.
It sets the key in its own process for that one command, never in your
shell. It builds `./cmd/cacheservice` with `go` first (set `CACHESERVICE` to
a binary to skip that), and runs it in your current directory, so a
relative `-tokens-file` works. Its flags go after the action, as for the
command itself. It prints the bucket and prefix that `-confirm` must repeat.

A fresh bucket, in this order (with the workspace's `TF_WORKSPACE`):

```bash
./hack/remote-cache/spaces/spaces.sh lifecycle claim -confirm=<bucket>/<prefix>
./hack/remote-cache/spaces/spaces.sh lifecycle plan -tokens-file tokens
./hack/remote-cache/spaces/spaces.sh lifecycle apply -tokens-file tokens -confirm=<bucket>/<prefix>
./hack/remote-cache/spaces/spaces.sh lifecycle check -tokens-file tokens
```

`claim` creates the prefix's ownership marker, which `apply` requires; a
prefix a writer already started on doesn't need it. Without `-tokens-file`
the one namespace is `default`. Run `apply` again when the tokens file's
namespaces change.

## Tear down

Destroys the bucket and everything in it, then the keys:

```bash
./hack/remote-cache/spaces/spaces.sh down
```

The slug lives in the state, so `down` needs the same state directory and
workspace (`TF_WORKSPACE`) as `up`.

## Things to know

- The admin key is account-wide, and each state makes one. It stays out of
  GitHub: there is no script for it. Use it only through `spaces.sh lifecycle`
  (or `spaces.sh env admin`, interactively).
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
