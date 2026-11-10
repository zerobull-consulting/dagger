locals {
  endpoint = "https://${var.region}.digitaloceanspaces.com"

  common = {
    CACHESERVICE_S3_ENDPOINT = local.endpoint
    CACHESERVICE_S3_REGION   = var.region
    CACHESERVICE_S3_BUCKET   = digitalocean_spaces_bucket.cache.name
    CACHESERVICE_S3_PREFIX   = var.prefix
  }
}

output "bucket" {
  value = digitalocean_spaces_bucket.cache.name
}

output "endpoint" {
  value = local.endpoint
}

output "prefix" {
  value = var.prefix
}

# For `spaces.sh lifecycle check`, which checks the rules main.tf set.
output "namespaces" {
  value = distinct(var.namespaces)
}

output "github_environments" {
  value = local.github_repository == null ? null : {
    write = local.github_write_env
    read  = local.github_read_env
  }
}

# Secrets are sensitive: read them with `terraform output -json <name>`,
# or use ./spaces.sh env <admin|write|read>.
output "admin" {
  sensitive = true
  value = merge(local.common, {
    CACHESERVICE_S3_ACCESS_KEY_ID     = digitalocean_spaces_key.admin.access_key
    CACHESERVICE_S3_SECRET_ACCESS_KEY = digitalocean_spaces_key.admin.secret_key
  })
}

output "write" {
  sensitive = true
  value = merge(local.common, {
    CACHESERVICE_S3_ACCESS_KEY_ID     = digitalocean_spaces_key.write.access_key
    CACHESERVICE_S3_SECRET_ACCESS_KEY = digitalocean_spaces_key.write.secret_key
  })
}

output "read" {
  sensitive = true
  value = merge(local.common, {
    CACHESERVICE_S3_ACCESS_KEY_ID     = digitalocean_spaces_key.read.access_key
    CACHESERVICE_S3_SECRET_ACCESS_KEY = digitalocean_spaces_key.read.secret_key
  })
}
