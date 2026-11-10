# Spaces bucket for a repository (Terraform)

Sets up a DigitalOcean Spaces bucket for the remote cache from scratch, and
removes it again, in one Terraform configuration. It creates:

- the bucket and the three keys the service documents
  ([The keys](../../../cmd/cacheservice/README.md#the-keys));
- the prefix's ownership marker and the bucket's lifecycle rules, through
  `cacheservice provision`, so the service's own code claims the prefix and
  sets the rules, with its checks;
- for a GitHub repository: an Actions environment for the write key and one
  for the read key, with each key's settings as secrets and variables.

Then a repository's jobs only pick their environment. Use one bucket per
repository, or a throwaway one for the tests of
[Testing against a real bucket](../README.md#testing-against-a-real-bucket).

| Key | Spaces grant | Use |
| --- | --- | --- |
| `-admin` | full access (account-wide: Spaces can't scope it to a bucket) | Terraform, for the bucket, the marker and the lifecycle rules. Never in CI. |
| `-write` | `readwrite` on the bucket | The writer service, export-capable jobs, GC. |
| `-read` | `read` on the bucket | Pull-request jobs, `-export=false`. Can't write. |

The bucket is private and has `force_destroy`: **tear-down deletes its
objects.** The name is generated, so it is a fresh bucket each time.

## Set up a repository

Keep each repository's variables in a `.tfvars` file (git-ignored), and its
state in a Terraform workspace of its own (`TF_WORKSPACE`; the first command
creates it):

```hcl
# dagger.tfvars
github_repository = "zerobull-consulting/dagger"
# The defaults, shown for clarity:
github_write_environment = "main"
github_read_environment  = "pull-requests"
github_write_branches    = ["main"]
namespaces               = ["default"]
```

```bash
export DIGITALOCEAN_TOKEN=... GITHUB_TOKEN=... TF_WORKSPACE=dagger
./hack/remote-cache/spaces/spaces.sh up -var-file=dagger.tfvars
```

`up` is `terraform init` and `apply`, and safe to run again: Terraform changes
only what differs. It needs:

- `terraform` 1.7 or later (or `TF=tofu`);
- one DigitalOcean API token, with read and write scopes, as `do_token`:
  `DIGITALOCEAN_TOKEN` or `TF_VAR_do_token` in the environment, or in a
  `terraform.tfvars` here (git-ignored, loaded by Terraform itself);
- `go`, since Terraform runs `cacheservice provision` from this checkout with
  `go run` (set `cacheservice_command` to a built binary's path to skip
  that);
- with `github_repository` set, a GitHub token with admin rights on the
  repository, in `GITHUB_TOKEN`. The GitHub provider creates environments and
  sets Actions secrets.

`terraform plan` shows the drift that `up` would repair: a lifecycle rule
changed or removed by hand shows as an update of `shell_script.rules`, and a
missing marker as a create of `shell_script.claim`.

Then a job picks its environment and reads the same names whichever key it
holds:

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

## GitHub

With `github_repository` set (`github.tf`):

- **Environments.** `main` and `pull-requests` by default
  (`github_write_environment`, `github_read_environment`). Terraform creates
  them, and limits the write key's to the branches in `github_write_branches`
  (`main`), so no other branch's workflow can take the write key. With
  `github_create_environments = false`, they must exist already, and are left
  alone (no branch policy is set then). **Destroying an environment deletes
  everything in it**, not only what this configuration set: set
  `github_create_environments = false` for environments that hold other
  secrets.
- **Secrets and variables.** In each environment, `<prefix>ACCESS_KEY_ID` and
  `<prefix>SECRET_ACCESS_KEY` as secrets, and `<prefix>ENDPOINT`, `_REGION`,
  `_BUCKET` and `_PREFIX` as variables (as secrets with `github_all_secrets`;
  `_PREFIX` is left out when the prefix is empty, since GitHub refuses an empty
  value). The prefix is `CACHE_S3_` for both keys
  (`github_write_name_prefix`, `github_read_name_prefix`), the names the
  [remote-cache action's example](../../../.github/actions/remote-cache/README.md)
  reads.
- **Repository-level instead.** An empty environment
  (`-var github_read_environment=`) puts that key's settings at repository
  level. Every workflow on every branch can read those, so give the two keys
  different name prefixes then; `plan` refuses both keys under the same names
  in the same place.
- The admin key never goes to GitHub.

A secret changed in GitHub by hand is set back on the next `up`. For this
repository's [Spaces tests](../../../.github/workflows/remote-cache-spaces.yml),
which read every `SPACES_*` as a repository secret:
`-var github_write_environment= -var github_read_environment= -var github_write_name_prefix=SPACES_ -var github_read_name_prefix=SPACES_READ_ -var github_all_secrets=true`.

## The marker and the lifecycle rules

Two `shell_script` resources of the
[shell provider](https://registry.terraform.io/providers/scottwinkler/shell)
run `cacheservice provision` (`cmd/cacheservice/provision`) with the admin
key, from this directory:

- **`shell_script.claim`** is `lifecycle claim`: it creates the prefix's
  ownership marker, which a writer's startup check and the rules need, and
  refuses a prefix that holds keys and no marker. Destroying it leaves the
  marker; destroying the bucket removes it.
- **`shell_script.rules`** is `lifecycle apply -allow-removals` for
  `namespaces`: one staging rule per namespace, and the rule aborting
  incomplete multipart uploads (`abort_rule`). It merges with any other
  tool's rules, writes only rules that can't expire live data, and reads them
  back. Its read reports whether the bucket holds exactly these rules, so
  drift shows in `plan`. Destroying it removes this prefix's rules only.

The sidecar action's namespace is its `namespace` input, `default` by
default, so `namespaces = ["default"]` covers it. A service with a tokens
file lists the file's namespaces here.

To look at the rules from your own shell, with the admin key and the
namespaces Terraform set:

```bash
./hack/remote-cache/spaces/spaces.sh lifecycle plan    # expect every line to be =
./hack/remote-cache/spaces/spaces.sh lifecycle check
```

## Keys and tests

```bash
eval "$(./hack/remote-cache/spaces/spaces.sh env write)"   # or read, or admin
export CACHESERVICE_TEST_EXTERNAL=1 CACHESERVICE_TEST_STORE=spaces CACHESERVICE_S3_PREFIX=test
```

The bucket is named `dagger-rc-test-<6 random characters>`
(`bucket_name_prefix` changes the prefix; `-var bucket_name=...` sets the whole
name). In a workspace, the keys are named `dagger-rc-<workspace>-admin`,
`-write` and `-read`, so the control panel says which is which
(`key_name_prefix` overrides it).

## Tear down

Destroys the GitHub secrets, variables and environments, this prefix's rules,
then the bucket and everything in it, then the keys:

```bash
./hack/remote-cache/spaces/spaces.sh down -var-file=dagger.tfvars
```

`down` needs the same workspace (`TF_WORKSPACE`) and variables as `up`.

## Things to know

- The admin key is account-wide, and each state makes one. It is only in
  Terraform's state and the `provision` commands' environment.
- Terraform state holds the keys' secrets. It is local and git-ignored; treat
  the directory as sensitive, and run `down` before discarding it. Losing the
  state leaves the bucket, the keys and the GitHub settings behind, to delete
  by hand.
- Spaces keys are scoped to a bucket, not a prefix. The service and the tests
  keep to their prefix themselves.
- The bucket's own resource ignores its lifecycle rules (`ignore_changes`):
  `shell_script.rules` owns them.
- Not yet applied against a real DigitalOcean account or GitHub repository.
  Before relying on it, run a first `up` in a throwaway workspace, and check
  that a `-read` key can't write.
