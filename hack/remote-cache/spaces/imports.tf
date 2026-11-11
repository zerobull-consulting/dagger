# Adopt the Actions variables that already exist under the names github.tf
# sets, such as those an earlier `gh variable set` made. GitHub creates a
# variable with a POST that fails with 409 when it exists (secrets and
# environments are PUTs, which overwrite, so they need no import).
#
# Only what exists is imported: the data sources list the repository's
# environments, then each existing environment's variables, so a new
# environment or repository imports nothing. A variable already in the state
# makes its import block a no-op, so these stay. Needs Terraform 1.7 (import
# with for_each).

data "github_repository_environments" "existing" {
  count      = local.github_repository == null ? 0 : 1
  repository = local.github_repo
}

locals {
  # The environments github.tf puts variables in that exist already.
  github_existing_environments = local.github_repository == null ? toset([]) : toset([
    for e in data.github_repository_environments.existing[0].environments : e.name
    if contains([for s in values(local.github_settings) : s.environment if s.environment != null && !s.secret], e.name)
  ])
  github_has_repo_variables = length([for s in values(local.github_settings) : s if s.environment == null && !s.secret]) > 0
}

data "github_actions_environment_variables" "existing" {
  for_each    = local.github_existing_environments
  name        = local.github_repo
  environment = each.value
}

data "github_actions_variables" "existing" {
  count = local.github_has_repo_variables ? 1 : 0
  name  = local.github_repo
}

locals {
  # Import IDs: <repo>:<environment>:<name>, or <repo>:<name> at repository
  # level, with a ':' inside a part written as '??' (the provider's escaping).
  github_existing_variables = merge(
    { for e, d in data.github_actions_environment_variables.existing : e => [for v in d.variables : v.name] },
    { "" = local.github_has_repo_variables ? [for v in data.github_actions_variables.existing[0].variables : v.name] : [] },
  )
}

import {
  for_each = {
    for k, s in local.github_settings : k => s
    if s.environment != null && !s.secret && contains(lookup(local.github_existing_variables, s.environment, []), s.name)
  }
  to = github_actions_environment_variable.v[each.key]
  id = "${local.github_repo}:${replace(each.value.environment, ":", "??")}:${each.value.name}"
}

import {
  for_each = {
    for k, s in local.github_settings : k => s
    if s.environment == null && !s.secret && contains(local.github_existing_variables[""], s.name)
  }
  to = github_actions_variable.v[each.key]
  id = "${local.github_repo}:${each.value.name}"
}
