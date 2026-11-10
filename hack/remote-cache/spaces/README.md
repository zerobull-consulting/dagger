# Spaces bucket for a repository (Terraform)

Local utilities that set up a DigitalOcean Spaces bucket for the remote cache
from scratch: the bucket and the three keys the service documents
([The keys](../../../cmd/cacheservice/README.md#the-keys)), a tokens file,
the lifecycle rules, and the GitHub Actions secrets and variables of a
repository's environments. They also remove it again. One bucket per
repository, or a throwaway one for the tests of
[Testing against a real bucket](../README.md#testing-against-a-real-bucket).

| Key | Spaces grant | Use |
| --- | --- | --- |
| `-admin` | full access (account-wide: Spaces can't scope it to a bucket) | `cacheservice lifecycle apply`, interactively. Never in CI. |
| `-write` | `readwrite` on the bucket | The writer service, export-capable jobs, GC. |
| `-read` | `read` on the bucket | Pull-request jobs, `-export=false`. Can't write. |

The bucket is private and has `force_destroy`: **tear-down deletes its
objects.** The name is generated, so it is a fresh bucket each time.

## Set up a repository in one command

`spaces.sh setup` does everything a repository's Actions jobs need, in order:
creates the bucket and keys (`up`), claims the prefix and sets its lifecycle
rules (`lifecycle claim`, `apply`, `check`, with the admin key), then stores
the write and the read key's settings in the repository's environments
(`github write`, `github read`). It is safe to run again: each step is a
no-op when already done. Keep each repository's variables in a `.tfvars`
file (git-ignored) and its state in a workspace of its own:

```hcl
# dagger.tfvars
github_repository        = "zerobull-consulting/dagger"
github_write_environment = "main"
github_read_environment  = "pull-requests"
github_read_name_prefix  = "CACHE_S3_"
```

```bash
export DIGITALOCEAN_TOKEN=... TF_WORKSPACE=dagger
./hack/remote-cache/spaces/spaces.sh setup -var-file=dagger.tfvars
```

It needs `terraform`, `go` and `gh` (logged in, with access to the
repository's secrets). Without `github_repository`, it skips GitHub. If
`apply` would remove a namespace's rule, `setup` stops there: run
`lifecycle apply -allow-removals` yourself (see [Lifecycle rules](#lifecycle-rules)).

Then the jobs only pick their environment, and read the same names
whichever key it holds:

```yaml
jobs:
  build:
    # main for pushes to the default branch, pull-requests for pull requests
    environment: ${{ github.event_name == 'pull_request' && 'pull-requests' || 'main' }}
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@v5
      - uses: zerobull-consulting/dagger/.github/actions/remote-cache/start@<commit SHA>
        with:
          role: ${{ github.event_name == 'pull_request' && 'read-only' || 'writer' }}
          s3-endpoint: ${{ vars.CACHE_S3_ENDPOINT }}
          s3-region: ${{ vars.CACHE_S3_REGION }}
          s3-bucket: ${{ vars.CACHE_S3_BUCKET }}
          s3-prefix: ${{ vars.CACHE_S3_PREFIX }}
          s3-access-key-id: ${{ secrets.CACHE_S3_ACCESS_KEY_ID }}
          s3-secret-access-key: ${{ secrets.CACHE_S3_SECRET_ACCESS_KEY }}
```

Restrict the `main` environment to the default branch (Settings,
Environments, deployment branches), or any branch's workflow can take the
write key.

The sections below are the steps `setup` runs, one at a time.

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

## Tokens

The sidecar action doesn't use tokens: it runs the service with `-scope`
(its `namespace` input, `default` by default) and takes any engine token.
A tokens file matters for a service shared between runners, which turns on
authentication with `-tokens-file`, and for the lifecycle rules: they are one
per namespace, and `lifecycle` takes the namespaces from the tokens file
([The tokens file](../../../cmd/cacheservice/README.md#the-tokens-file)).

Each workspace has its own, `tokens/<workspace>.tokens` in this directory
(git-ignored, mode 0600; `TOKENS_FILE` overrides it):

```bash
./hack/remote-cache/spaces/spaces.sh tokens add team-a      # prints the new token, once
./hack/remote-cache/spaces/spaces.sh tokens list            # fingerprints and namespaces, no tokens
./hack/remote-cache/spaces/spaces.sh tokens revoke 1a99fa0a # by fingerprint, or by namespace: all of its tokens
./hack/remote-cache/spaces/spaces.sh tokens path
```

`add` generates a token as the service's README recommends (`openssl rand
-hex 32`), checks the namespace by the service's rule, and prints the token
on standard output (`tok="$(spaces.sh tokens add team-a)"`), the rest on
standard error. The fingerprints are the ones the service logs. After `add`
of a new namespace, run `lifecycle apply` (or `setup`) for its rule; after
`revoke`, send the service `SIGHUP`.

## Lifecycle rules

`spaces.sh lifecycle` runs [`cacheservice lifecycle`](../../../cmd/cacheservice/README.md#the-lifecycle-command)
with the admin key, the only one Spaces lets read or write lifecycle rules.
It sets the key in a subshell for that one command, never in your shell. It
builds `./cmd/cacheservice` with `go` first (set `CACHESERVICE` to a binary
to skip that), and runs it in your current directory, so a relative
`-tokens-file` works. Its flags go after the action, as for the command
itself. It prints the bucket and prefix that `-confirm` must repeat.

`plan`, `apply` and `check` take the namespaces from the workspace's tokens
file when it exists and neither `-tokens-file` nor `-scope` is given;
otherwise the one namespace is `default`, the sidecar action's. `claim`
reads none. By hand, in this order (what `setup` runs):

```bash
./hack/remote-cache/spaces/spaces.sh lifecycle claim -confirm=<bucket>/<prefix>
./hack/remote-cache/spaces/spaces.sh lifecycle plan
./hack/remote-cache/spaces/spaces.sh lifecycle apply -confirm=<bucket>/<prefix>
./hack/remote-cache/spaces/spaces.sh lifecycle check
```

`claim` creates the prefix's ownership marker, which `apply` requires; a
prefix a writer already started on doesn't need it. Run `apply` again when
the namespaces change; one that would remove a rule needs
`-allow-removals`. `setup` passes `-confirm` itself, since it only runs on
the bucket its own state created; by hand you type it.

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
