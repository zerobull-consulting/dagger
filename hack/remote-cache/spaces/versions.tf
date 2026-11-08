terraform {
  required_version = ">= 1.5"

  required_providers {
    digitalocean = {
      source = "digitalocean/digitalocean"
      # digitalocean_spaces_key arrived in 2.54.
      version = ">= 2.54"
    }
    random = {
      source  = "hashicorp/random"
      version = ">= 3.5"
    }
  }
}

# The one input is a DigitalOcean API token (var.do_token). It creates the keys;
# the admin key it makes then creates and removes the bucket, so no Spaces key
# has to be supplied by hand.
provider "digitalocean" {
  token = var.do_token
}

provider "digitalocean" {
  alias             = "spaces"
  token             = var.do_token
  spaces_access_id  = digitalocean_spaces_key.admin.access_key
  spaces_secret_key = digitalocean_spaces_key.admin.secret_key
}
