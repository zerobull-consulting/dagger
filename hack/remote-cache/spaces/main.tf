# A dedicated Spaces bucket, ready for the remote cache service: the three
# keys it documents (cmd/cacheservice/README.md, "The keys"), the prefix's
# ownership marker, and the lifecycle rules `cacheservice lifecycle apply`
# would set. The GitHub side is github.tf.
#
# force_destroy lets `terraform destroy` delete a bucket that still holds
# cache objects. Don't point this configuration at a bucket you care about:
# it owns the bucket's whole lifecycle configuration, and writes the marker
# without the checks `lifecycle claim` makes (see the marker below).

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
  # The service's layout (cmd/cacheservice/layout): its root is the prefix
  # with one trailing slash, or "" for the whole bucket.
  root       = trimspace(trim(var.prefix, "/")) == "" ? "" : "${trim(var.prefix, "/")}/"
  marker_key = "${local.root}cacheservice.json"
  # layout.Version. terraform_test.go fails when they differ.
  marker_layout = 2

  # cacheservice lifecycle's rules (lifecycle.Rules): one per namespace,
  # expiring its staged uploads after a day, and one aborting incomplete
  # multipart uploads under the prefix after a day. The IDs carry 8 hex
  # digits of the root's SHA-256, as the command's do, so `lifecycle plan`
  # and `check` see them as its own. terraform_test.go compares them.
  rule_tag = substr(sha256(local.root), 0, 8)
  lifecycle_rules = concat(
    [for ns in distinct(var.namespaces) : {
      id              = "cacheservice-${local.rule_tag}-staging-${ns}"
      prefix          = "${local.root}ns/${ns}/staging/"
      expiration_days = 1
      abort_days      = null
    }],
    var.abort_rule ? [{
      id              = "cacheservice-${local.rule_tag}-incomplete-uploads"
      prefix          = local.root
      expiration_days = null
      abort_days      = 1
    }] : [],
  )

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

  # The bucket's whole lifecycle configuration: rules set by anything else
  # are removed on the next apply.
  dynamic "lifecycle_rule" {
    for_each = local.lifecycle_rules
    content {
      id                                     = lifecycle_rule.value.id
      prefix                                 = lifecycle_rule.value.prefix
      enabled                                = true
      abort_incomplete_multipart_upload_days = lifecycle_rule.value.abort_days

      dynamic "expiration" {
        for_each = lifecycle_rule.value.expiration_days == null ? [] : [lifecycle_rule.value.expiration_days]
        content {
          days = expiration.value
        }
      }
    }
  }
}

# The prefix's ownership marker (ownership.Marker), which a writer's startup
# check and `lifecycle apply` require: what `cacheservice lifecycle claim`
# writes. claim also refuses a prefix that already holds keys, and writes
# with If-None-Match; Terraform does neither, which is safe only because
# the bucket above is this state's own and new. Don't import a bucket that
# is in use.
resource "time_static" "marker_created" {}

resource "digitalocean_spaces_bucket_object" "marker" {
  provider     = digitalocean.spaces
  region       = var.region
  bucket       = digitalocean_spaces_bucket.cache.name
  key          = local.marker_key
  content_type = "application/json"
  acl          = "private"
  content = jsonencode({
    layout         = local.marker_layout
    created        = time_static.marker_created.rfc3339
    serviceVersion = "terraform"
  })
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
