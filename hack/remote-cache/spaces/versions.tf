terraform {
  required_version = ">= 1.5"

  required_providers {
    digitalocean = {
      source = "digitalocean/digitalocean"
      # digitalocean_spaces_key arrived in 2.54.
      version = ">= 2.54"
    }
    github = {
      source = "integrations/github"
      # github_actions_*_secret's value arrived in 6.13 (plaintext_value is deprecated).
      version = ">= 6.13"
    }
    shell = {
      source = "scottwinkler/shell"
      # Runs cacheservice provision for the marker and the rules (main.tf).
      version = ">= 1.7.10"
    }
    random = {
      source  = "hashicorp/random"
      version = ">= 3.5"
    }
  }
}

# The DigitalOcean input is one API token (var.do_token). It creates the keys;
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

# Takes its token from GITHUB_TOKEN (or `gh auth token`), with admin rights
# on the repository: it creates environments and sets Actions secrets.
# Unused, and not needed, when github_repository is unset.
provider "github" {
  owner = local.github_repository == null ? null : split("/", local.github_repository)[0]
}
