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
  description = "CACHESERVICE_S3_PREFIX. Informational here: it only shapes the outputs, since Spaces keys are scoped to a bucket, not a prefix."
  type        = string
  default     = "cache"
}

variable "key_name_prefix" {
  description = "Names of the three keys are <this>-admin, -write and -read. Null: dagger-rc-<workspace>, or dagger-remote-cache-test in the default workspace."
  type        = string
  default     = null
}

# Where the gh_secret_set output puts the settings. One state (directory or
# workspace) per repository and environment: see README.md, "Into GitHub".

variable "github_repository" {
  description = "owner/name the gh_secret_set output targets (gh's --repo). Null: the repository gh finds in the current directory."
  type        = string
  default     = null

  validation {
    condition     = var.github_repository == null || can(regex("^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", var.github_repository))
    error_message = "github_repository must look like owner/name."
  }
}

variable "github_environment" {
  description = "GitHub Actions environment the gh_secret_set output targets (gh's --env). Null: repository-level secrets and variables."
  type        = string
  default     = null
}

variable "github_key" {
  description = "Which key gh_secret_set stores: write (a writer, such as the default branch's jobs) or read (pull-request jobs, a read-only sidecar). Never admin."
  type        = string
  default     = "write"

  validation {
    condition     = contains(["write", "read"], var.github_key)
    error_message = "github_key must be write or read: the admin key never goes to CI."
  }
}

variable "github_name_prefix" {
  description = "Prefix of the secret and variable names gh_secret_set sets. CACHE_S3_ matches the remote-cache action's README; SPACES_ matches remote-cache-spaces.yml (set github_all_secrets too)."
  type        = string
  default     = "CACHE_S3_"
}

variable "github_all_secrets" {
  description = "Store ENDPOINT, REGION, BUCKET and PREFIX as secrets too, instead of variables (gh variable set). remote-cache-spaces.yml reads every SPACES_* as a secret."
  type        = bool
  default     = false
}
