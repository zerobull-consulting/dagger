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
  description = "Names of the three keys are <this>-admin, -write and -read."
  type        = string
  default     = "dagger-remote-cache-test"
}
