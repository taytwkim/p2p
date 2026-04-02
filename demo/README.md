# Demo Automation

This directory contains demo assets for `p2pfs`.

## RPC Demo Script

To run the daemon + RPC demo (which spins up Peer A, Peer B, and Peer C, and generates a test `foo.txt` file):
```bash
./demo/rpc_demo.sh start
```

Watch the terminal as it starts the daemons. Once it completes, first list a remote peer to see the CID for `foo.txt`, then use that CID from Peer C.

```bash
./p2pfs list   --rpc /tmp/p2pfsC.sock --peer <REMOTE_MULTIADDR>
./p2pfs whohas --rpc /tmp/p2pfsC.sock <CID>
./p2pfs fetch  --rpc /tmp/p2pfsC.sock <CID>
cat peerC_export/foo.txt
```

To clean up the spawned log files, temp socket files, `export` directories, and abruptly kill all `p2pfs` processes:

```bash
./demo/rpc_demo.sh clean
```

## GCP Cross-VM Demo

Terraform files for a simple 3-VM GCP demo environment live under `demo/gcp`.

The Terraform stack creates:

- `p2pfs-bootstrap`
- `p2pfs-peer-b`
- `p2pfs-peer-c`

along with a firewall rule that opens:

- `tcp:22` for SSH
- `tcp:4001-4010` for libp2p demo traffic

After `terraform apply`, SSH into the VMs with `gcloud compute ssh`, clone this repo, and run `./p2pfs shell ...` manually for the presentation.

Suggested flow:

1. `cd demo/gcp`
2. `cp terraform.tfvars.example terraform.tfvars`
3. Fill in your GCP project and preferred zone.
4. `terraform init`
5. `terraform apply`
6. SSH into the three VMs with the `gcloud compute ssh ...` commands from Terraform outputs.
7. Clone this repo on each VM and run `./p2pfs shell ...` manually for the live demo.

Use `terraform destroy` when you are done.