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

# Empty when unset, so spaces.sh setup can tell (it skips GitHub then).
output "github_repository" {
  value = var.github_repository == null ? "" : var.github_repository
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

# Two bash scripts, one per key, that store the key's settings in a GitHub
# repository or one of its environments with gh: the key ID and secret as
# secrets, the endpoint, region, bucket and prefix as variables (or secrets,
# with github_all_secrets). The write key and the read key each go to their
# own environment, or under their own names; the admin key never goes to
# GitHub. Run one with ./spaces.sh github write|read, or
# `terraform output -raw gh_secret_set_write | bash`. Each value goes to gh
# on stdin from printf, a builtin, so no secret lands on a command line.
locals {
  github = {
    write = {
      key         = digitalocean_spaces_key.write
      environment = var.github_write_environment
      name_prefix = var.github_write_name_prefix
    }
    read = {
      key         = digitalocean_spaces_key.read
      environment = var.github_read_environment
      name_prefix = var.github_read_name_prefix
    }
  }

  # Both keys' settings would land under the same names in the same place.
  github_collision = (
    var.github_write_environment == var.github_read_environment &&
    var.github_write_name_prefix == var.github_read_name_prefix
  )

  gh_secret_set = { for role, g in local.github : role => join("\n", concat(
    ["set -euo pipefail"],
    [for s in [
      { name = "ENDPOINT", value = local.endpoint, secret = var.github_all_secrets },
      { name = "REGION", value = var.region, secret = var.github_all_secrets },
      { name = "BUCKET", value = digitalocean_spaces_bucket.cache.name, secret = var.github_all_secrets },
      { name = "PREFIX", value = var.prefix, secret = var.github_all_secrets },
      { name = "ACCESS_KEY_ID", value = g.key.access_key, secret = true },
      { name = "SECRET_ACCESS_KEY", value = g.key.secret_key, secret = true },
      ] : format(
      "printf '%%s' '%s' | gh %s set %s%s%s%s",
      replace(s.value, "'", "'\\''"),
      s.secret ? "secret" : "variable",
      g.name_prefix,
      s.name,
      var.github_repository == null ? "" : " --repo ${var.github_repository}",
      g.environment == null ? "" : " --env '${replace(g.environment, "'", "'\\''")}'",
    )],
    [""],
  )) }
}

output "gh_secret_set_write" {
  description = "bash script: store the write key's settings in GitHub (github_write_environment, github_write_name_prefix)."
  sensitive   = true
  value       = local.gh_secret_set.write

  precondition {
    condition     = !local.github_collision
    error_message = "The write and read keys would be stored under the same names in the same place: give them different github_*_environment values or different github_*_name_prefix values."
  }
}

output "gh_secret_set_read" {
  description = "bash script: store the read key's settings in GitHub (github_read_environment, github_read_name_prefix)."
  sensitive   = true
  value       = local.gh_secret_set.read

  precondition {
    condition     = !local.github_collision
    error_message = "The write and read keys would be stored under the same names in the same place: give them different github_*_environment values or different github_*_name_prefix values."
  }
}
