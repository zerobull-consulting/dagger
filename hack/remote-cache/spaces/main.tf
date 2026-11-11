# A dedicated Spaces bucket, ready for the remote cache service: the three
# keys it documents (cmd/cacheservice/README.md, "The keys"), the prefix's
# ownership marker and the bucket's lifecycle rules. The marker and the rules
# are `cacheservice provision` resources, run through the shell provider, so
# the service's own code claims the prefix and sets the rules, with its
# checks (cmd/cacheservice/provision). The GitHub side is github.tf.
#
# force_destroy lets `terraform destroy` delete a bucket that still holds
# cache objects. Don't point this configuration at a bucket you care about.

resource "random_string" "slug" {
  length  = 6
  lower   = true
  upper   = false
  numeric = true
  special = false
}

locals {
  bucket_name = coalesce(var.bucket_name, "${var.bucket_name_prefix}-${random_string.slug.result}")

  # Name the keys after the workspace (one per repository and environment,
  # see README.md), so the control panel's key list says which is which. Not
  # after the github_* variables: changing those only changes an output.
  key_name_prefix = coalesce(
    var.key_name_prefix,
    terraform.workspace == "default" ? "dagger-remote-cache-test" : "dagger-rc-${terraform.workspace}",
  )
}

# Created with the admin key (provider alias), so destroy removes the bucket
# before the key.
resource "digitalocean_spaces_bucket" "cache" {
  provider      = digitalocean.spaces
  name          = local.bucket_name
  region        = var.region
  acl           = "private"
  force_destroy = true

  versioning {
    enabled = false
  }

  # cacheservice provision rules (below) owns this prefix's rules, merged
  # with any others: don't let the bucket resource remove them.
  lifecycle {
    ignore_changes = [lifecycle_rule]
  }
}

# Admin: lifecycle rules (`cacheservice lifecycle apply`). Spaces gives a
# limited key a 403 on lifecycle writes, so this must be full access.
# DigitalOcean cannot scope full access to one bucket: it covers every
# bucket in the account. Keep it out of CI and the service's environment.
resource "digitalocean_spaces_key" "admin" {
  name = "${local.key_name_prefix}-admin"

  grant {
    bucket     = ""
    permission = "fullaccess"
  }
}

# Write: the everyday key for the writer service, export-capable jobs and GC.
resource "digitalocean_spaces_key" "write" {
  name = "${local.key_name_prefix}-write"

  grant {
    bucket     = digitalocean_spaces_bucket.cache.name
    permission = "readwrite"
  }
}

# Read: pull-request jobs and read-only instances (-export=false). Cannot
# write, so it cannot contribute back to the remote.
resource "digitalocean_spaces_key" "read" {
  name = "${local.key_name_prefix}-read"

  grant {
    bucket     = digitalocean_spaces_bucket.cache.name
    permission = "read"
  }
}

# `cacheservice provision` resources (shell_script: see
# cmd/cacheservice/provision for the contract). They use the admin key: on
# Spaces, the only one that can read and write lifecycle rules.
locals {
  provision = "${var.cacheservice_command} provision"

  provision_environment = {
    CACHESERVICE_S3_ENDPOINT = local.endpoint
    CACHESERVICE_S3_REGION   = var.region
    CACHESERVICE_S3_BUCKET   = digitalocean_spaces_bucket.cache.name
    CACHESERVICE_S3_PREFIX   = var.prefix
  }
  provision_secrets = {
    CACHESERVICE_S3_ACCESS_KEY_ID     = digitalocean_spaces_key.admin.access_key
    CACHESERVICE_S3_SECRET_ACCESS_KEY = digitalocean_spaces_key.admin.secret_key
  }
}

# The prefix's ownership marker, which a writer's startup check and the rules
# require: `lifecycle claim`, which refuses a prefix that already holds keys.
# Destroying it leaves the marker (the bucket's deletion removes it).
resource "shell_script" "claim" {
  lifecycle_commands {
    create = "${local.provision} claim create"
    read   = "${local.provision} claim read"
    delete = "${local.provision} claim delete"
  }
  environment           = local.provision_environment
  sensitive_environment = local.provision_secrets
  working_directory     = path.module

  # Another bucket or prefix is another claim.
  triggers = {
    bucket = digitalocean_spaces_bucket.cache.name
    prefix = var.prefix
  }
}

# One staging rule per namespace and the abort rule: `lifecycle apply
# -allow-removals`, merged with any other tool's rules. A rule changed or
# removed by hand shows as an update in the next plan.
resource "shell_script" "rules" {
  lifecycle_commands {
    create = "${local.provision} rules create"
    read   = "${local.provision} rules read"
    update = "${local.provision} rules update"
    delete = "${local.provision} rules delete"
  }
  environment = merge(local.provision_environment, {
    CACHESERVICE_NAMESPACES = join(",", distinct(var.namespaces))
    CACHESERVICE_ABORT_RULE = tostring(var.abort_rule)
  })
  sensitive_environment = local.provision_secrets
  working_directory     = path.module

  # The rules of another bucket or prefix are other rules: replacing this
  # resource removes the old prefix's first.
  triggers = {
    bucket = digitalocean_spaces_bucket.cache.name
    prefix = var.prefix
  }

  depends_on = [shell_script.claim]
}
