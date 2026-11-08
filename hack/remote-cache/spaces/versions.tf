terraform {
  required_version = ">= 1.5"

  required_providers {
    digitalocean = {
      source = "digitalocean/digitalocean"
      # digitalocean_spaces_key arrived in 2.54.
      version = ">= 2.54"
    }
  }
}

# Credentials come from the environment, never from files in this directory:
#   DIGITALOCEAN_TOKEN        a personal access token (manages the bucket and keys)
#   SPACES_ACCESS_KEY_ID      a Spaces key that can create buckets
#   SPACES_SECRET_ACCESS_KEY  (the provider reads both of these names)
provider "digitalocean" {}
