output "instance_public_ips" {
  description = "Public IPs for the demo VMs."
  value = {
    for name, instance in google_compute_instance.p2pfs_demo :
    name => instance.network_interface[0].access_config[0].nat_ip
  }
}

output "instance_names" {
  description = "Names of the demo VMs."
  value       = sort(keys(google_compute_instance.p2pfs_demo))
}

output "ssh_examples" {
  description = "Example gcloud SSH commands."
  value = {
    for name, instance in google_compute_instance.p2pfs_demo :
    name => "gcloud compute ssh ${name} --project ${var.project_id} --zone ${var.zone}"
  }
}
