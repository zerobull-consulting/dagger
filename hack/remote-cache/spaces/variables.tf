variable "region" {
  description = "Spaces region slug, such as nyc3 or sfo3."
  type        = string
  default     = "nyc3"
}

variable "bucket_name" {
  description = "Bucket name. Spaces names are unique within a region, so add your own suffix."
  type        = string
  default     = "dagger-remote-cache-test"
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
