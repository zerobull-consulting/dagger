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

# A bash script that stores one key's settings in a GitHub repository or
# environment with gh: the key ID and secret as secrets, the endpoint,
# region, bucket and prefix as variables (or secrets, with
# github_all_secrets). Run it with ./spaces.sh github, or
# `terraform output -raw gh_secret_set | bash`. Each value goes to gh on
# stdin from printf, a builtin, so no secret lands on a command line.
locals {
  github_key = var.github_key == "write" ? digitalocean_spaces_key.write : digitalocean_spaces_key.read

  github_target = join("", [
    var.github_repository == null ? "" : " --repo ${var.github_repository}",
    var.github_environment == null ? "" : " --env '${replace(var.github_environment, "'", "'\\''")}'",
  ])

  github_settings = [
    { name = "ENDPOINT", value = local.endpoint, secret = var.github_all_secrets },
    { name = "REGION", value = var.region, secret = var.github_all_secrets },
    { name = "BUCKET", value = digitalocean_spaces_bucket.cache.name, secret = var.github_all_secrets },
    { name = "PREFIX", value = var.prefix, secret = var.github_all_secrets },
    { name = "ACCESS_KEY_ID", value = local.github_key.access_key, secret = true },
    { name = "SECRET_ACCESS_KEY", value = local.github_key.secret_key, secret = true },
  ]
}

output "gh_secret_set" {
  sensitive = true
  value = join("\n", concat(
    ["set -euo pipefail"],
    [for s in local.github_settings : format(
      "printf '%%s' '%s' | gh %s set %s%s%s",
      replace(s.value, "'", "'\\''"),
      s.secret ? "secret" : "variable",
      var.github_name_prefix,
      s.name,
      local.github_target,
    )],
    [""],
  ))
}
