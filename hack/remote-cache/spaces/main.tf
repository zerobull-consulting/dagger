# A dedicated, disposable Spaces bucket and the three keys the remote cache
# service documents (cmd/cacheservice/README.md, "The keys").
#
# force_destroy lets `terraform destroy` delete a bucket that still holds
# cache objects. That is the point of a test bucket; do not point this
# configuration at a bucket you care about.

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
