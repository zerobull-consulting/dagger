# The repository's side: the write key's settings in one Actions environment
# (or repository-level), the read key's in another, and the environments
# themselves, the write one limited to github_write_branches. A job then only
# picks its environment and reads the same names. The admin key never goes
# to GitHub. Nothing here when github_repository is unset.
#
# Secrets go in as plain values; the provider encrypts them with the
# repository's public key. They are in the state too, as the keys already
# are. A secret changed in GitHub by hand is set back on the next apply.

locals {
  # "" is unset too, since -var can't say null.
  github_repository = var.github_repository == "" ? null : var.github_repository
  github_write_env  = var.github_write_environment == "" ? null : var.github_write_environment
  github_read_env   = var.github_read_environment == "" ? null : var.github_read_environment

  github_repo = local.github_repository == null ? null : split("/", local.github_repository)[1]

  github_keys = local.github_repository == null ? {} : {
    write = {
      key         = digitalocean_spaces_key.write
      environment = local.github_write_env
      name_prefix = var.github_write_name_prefix
    }
    read = {
      key         = digitalocean_spaces_key.read
      environment = local.github_read_env
      name_prefix = var.github_read_name_prefix
    }
  }

  # One entry per secret or variable. for_each can't take sensitive values,
  # so the entries hold no value; github_values maps each one to its value.
  github_settings = merge([for role, g in local.github_keys : {
    for s in [
      { name = "ENDPOINT", secret = var.github_all_secrets },
      { name = "REGION", secret = var.github_all_secrets },
      { name = "BUCKET", secret = var.github_all_secrets },
      { name = "PREFIX", secret = var.github_all_secrets },
      { name = "ACCESS_KEY_ID", secret = true },
      { name = "SECRET_ACCESS_KEY", secret = true },
      # GitHub refuses an empty value; an unset one reads as "" in a workflow.
      ] : "${role}/${s.name}" => {
      role        = role
      environment = g.environment
      name        = "${g.name_prefix}${s.name}"
      secret      = s.secret
    } if !(s.name == "PREFIX" && var.prefix == "")
  }]...)

  github_values = merge([for role, g in local.github_keys : {
    "${role}/ENDPOINT"          = local.endpoint
    "${role}/REGION"            = var.region
    "${role}/BUCKET"            = digitalocean_spaces_bucket.cache.name
    "${role}/PREFIX"            = var.prefix
    "${role}/ACCESS_KEY_ID"     = g.key.access_key
    "${role}/SECRET_ACCESS_KEY" = g.key.secret_key
  }]...)

  github_environments = var.github_create_environments ? toset(compact([
    for g in values(local.github_keys) : g.environment
  ])) : toset([])

  # The write environment's branch policy, unless the read key shares it
  # (pull requests must reach the read key).
  github_write_policy = (
    local.github_write_env != null &&
    contains(local.github_environments, coalesce(local.github_write_env, "-")) &&
    local.github_write_env != local.github_read_env &&
    length(var.github_write_branches) > 0
  )
}

# Both keys' settings would land under the same names in the same place.
resource "terraform_data" "github_check" {
  count = local.github_repository == null ? 0 : 1

  lifecycle {
    precondition {
      condition = !(
        local.github_write_env == local.github_read_env &&
        var.github_write_name_prefix == var.github_read_name_prefix
      )
      error_message = "The write and read keys would be stored under the same names in the same place: give them different github_*_environment values or different github_*_name_prefix values."
    }
  }
}

resource "github_repository_environment" "env" {
  for_each    = local.github_environments
  repository  = local.github_repo
  environment = each.value

  dynamic "deployment_branch_policy" {
    for_each = local.github_write_policy && each.value == local.github_write_env ? [1] : []
    content {
      protected_branches     = false
      custom_branch_policies = true
    }
  }
}

resource "github_repository_environment_deployment_policy" "write" {
  for_each       = local.github_write_policy ? toset(var.github_write_branches) : toset([])
  repository     = local.github_repo
  environment    = github_repository_environment.env[local.github_write_env].environment
  branch_pattern = each.value
}

# An environment name, through the environment resource when this
# configuration creates it, so secrets wait for their environment.
locals {
  github_environment_names = {
    for e in distinct(compact([for g in values(local.github_keys) : g.environment])) :
    e => var.github_create_environments ? github_repository_environment.env[e].environment : e
  }
}

resource "github_actions_environment_secret" "s" {
  for_each    = { for k, s in local.github_settings : k => s if s.environment != null && s.secret }
  repository  = local.github_repo
  environment = local.github_environment_names[each.value.environment]
  secret_name = each.value.name
  value       = local.github_values[each.key]
  depends_on  = [terraform_data.github_check]
}

resource "github_actions_environment_variable" "v" {
  for_each      = { for k, s in local.github_settings : k => s if s.environment != null && !s.secret }
  repository    = local.github_repo
  environment   = local.github_environment_names[each.value.environment]
  variable_name = each.value.name
  value         = local.github_values[each.key]
  depends_on    = [terraform_data.github_check]
}

resource "github_actions_secret" "s" {
  for_each    = { for k, s in local.github_settings : k => s if s.environment == null && s.secret }
  repository  = local.github_repo
  secret_name = each.value.name
  value       = local.github_values[each.key]
  depends_on  = [terraform_data.github_check]
}

resource "github_actions_variable" "v" {
  for_each      = { for k, s in local.github_settings : k => s if s.environment == null && !s.secret }
  repository    = local.github_repo
  variable_name = each.value.name
  value         = local.github_values[each.key]
  depends_on    = [terraform_data.github_check]
}
