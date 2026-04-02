provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

locals {
  instance_names = [
    "p2pfs-bootstrap",
    "p2pfs-peer-b",
    "p2pfs-peer-c",
  ]

  common_tags = ["p2pfs-demo"]
}

resource "google_compute_network" "p2pfs_demo" {
  name                    = "p2pfs-demo-network"
  auto_create_subnetworks = true
}

resource "google_compute_firewall" "p2pfs_demo_ingress" {
  name    = "p2pfs-demo-ingress"
  network = google_compute_network.p2pfs_demo.name

  allow {
    protocol = "tcp"
    ports    = ["22", "4001-4010"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = local.common_tags
}

resource "google_compute_instance" "p2pfs_demo" {
  for_each     = toset(local.instance_names)
  name         = each.value
  machine_type = var.machine_type
  zone         = var.zone
  tags         = local.common_tags

  boot_disk {
    initialize_params {
      image = "projects/debian-cloud/global/images/family/debian-12"
      size  = 20
      type  = "pd-balanced"
    }
  }

  network_interface {
    network = google_compute_network.p2pfs_demo.id

    access_config {}
  }

  metadata_startup_script = <<-EOT
    #!/bin/bash
    set -euxo pipefail
    export DEBIAN_FRONTEND=noninteractive
    apt-get update
    apt-get install -y git golang-go
  EOT
}
