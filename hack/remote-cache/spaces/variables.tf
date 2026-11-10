variable "region" {
  description = "Spaces region slug, such as nyc3 or sfo3."
  type        = string
  default     = "nyc3"
}

variable "do_token" {
  description = "DigitalOcean API token (read and write scopes). The only credential needed."
  type        = string
  sensitive   = true
}

variable "bucket_name_prefix" {
  description = "The bucket is named <this>-<6 random characters>, since Spaces names are unique within a region."
  type        = string
  default     = "dagger-rc-test"
}

variable "bucket_name" {
  description = "Set to use this exact name instead of the generated one."
  type        = string
  default     = null
}

variable "prefix" {
  description = "CACHESERVICE_S3_PREFIX: where the ownership marker and the lifecycle rules go. Spaces keys are scoped to a bucket, not a prefix, so the keys don't keep to it."
  type        = string
  default     = "cache"
}

variable "key_name_prefix" {
  description = "Names of the three keys are <this>-admin, -write and -read. Null: dagger-rc-<workspace>, or dagger-remote-cache-test in the default workspace."
  type        = string
  default     = null
}

# The service's prefix, as the bucket's lifecycle rules and ownership marker
# see it. See main.tf.

variable "namespaces" {
  description = "The namespaces the bucket's lifecycle rules cover, one staging rule each (cacheservice lifecycle's rules). The sidecar action's is its namespace input, default."
  type        = list(string)
  default     = ["default"]

  validation {
    condition     = alltrue([for ns in var.namespaces : can(regex("^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$", ns)) && !strcontains(ns, "..")])
    error_message = "A namespace is 1-128 letters, digits, '.', '_' or '-', starting with a letter or digit, without '..'."
  }
}

variable "abort_rule" {
  description = "Add the rule that aborts incomplete multipart uploads under the prefix (cacheservice lifecycle's -abort-rule)."
  type        = bool
  default     = true
}

# The GitHub repository whose Actions jobs use the bucket. See github.tf.

variable "github_repository" {
  description = "owner/name of the repository to store the write and read keys' settings in. Null or empty: no GitHub resources. The github provider takes its token from GITHUB_TOKEN."
  type        = string
  default     = null

  validation {
    condition     = var.github_repository == null || var.github_repository == "" || can(regex("^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", var.github_repository))
    error_message = "github_repository must look like owner/name."
  }
}

variable "github_write_environment" {
  description = "Actions environment for the write key, such as the default branch's. Null or empty: repository-level secrets and variables."
  type        = string
  default     = "main"
}

variable "github_read_environment" {
  description = "Actions environment for the read key, such as pull requests'. Null or empty: repository-level secrets and variables."
  type        = string
  default     = "pull-requests"
}

variable "github_create_environments" {
  description = "Create (and on destroy, delete) the two environments. false: they must exist already, and are left alone. Deleting an environment deletes everything in it, not only what this configuration set."
  type        = bool
  default     = true
}

variable "github_write_branches" {
  description = "Branches (fnmatch patterns) allowed to deploy to the write key's environment, when this configuration creates it and it isn't also the read key's. Empty: any branch."
  type        = list(string)
  default     = ["main"]
}

variable "github_write_name_prefix" {
  description = "Prefix of the write key's secret and variable names. CACHE_S3_ matches the remote-cache action's README; SPACES_ matches remote-cache-spaces.yml (set github_all_secrets too)."
  type        = string
  default     = "CACHE_S3_"
}

variable "github_read_name_prefix" {
  description = "Prefix of the read key's names. Must differ from github_write_name_prefix when both keys go to the same place."
  type        = string
  default     = "CACHE_S3_"
}

variable "github_all_secrets" {
  description = "Store ENDPOINT, REGION, BUCKET and PREFIX as secrets too, instead of variables. remote-cache-spaces.yml reads every SPACES_* as a secret."
  type        = bool
  default     = false
}
