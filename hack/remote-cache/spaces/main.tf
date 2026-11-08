# A dedicated, disposable Spaces bucket and the three keys the remote cache
# service documents (cmd/cacheservice/README.md, "The keys").
#
# force_destroy lets `terraform destroy` delete a bucket that still holds
# cache objects. That is the point of a test bucket; do not point this
# configuration at a bucket you care about.

resource "digitalocean_spaces_bucket" "cache" {
  name          = var.bucket_name
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
  name = "${var.key_name_prefix}-admin"

  grant {
    bucket     = ""
    permission = "fullaccess"
  }
}

# Write: the everyday key for the writer service, export-capable jobs and GC.
resource "digitalocean_spaces_key" "write" {
  name = "${var.key_name_prefix}-write"

  grant {
    bucket     = digitalocean_spaces_bucket.cache.name
    permission = "readwrite"
  }
}

# Read: pull-request jobs and read-only instances (-export=false). Cannot
# write, so it cannot contribute back to the remote.
resource "digitalocean_spaces_key" "read" {
  name = "${var.key_name_prefix}-read"

  grant {
    bucket     = digitalocean_spaces_bucket.cache.name
    permission = "read"
  }
}
