# Demo

This directory contains demo assets.

## Local RPC Demo

To run the local RPC demo (which spins up Peer A, Peer B, and Peer C, and generates a test `foo.txt` file):
```bash
# run from project root
./demo/rpc_demo.sh start
```

Watch the terminal as it starts the daemons. Once it completes, first list a remote peer to see the manifest CID for `foo.txt`, then use that manifest CID from Peer C.

```bash
./p2pfs list   --rpc /tmp/p2pfsC.sock --peer <REMOTE_MULTIADDR>
./p2pfs whohas --rpc /tmp/p2pfsC.sock <MANIFEST_CID>
./p2pfs fetch  --rpc /tmp/p2pfsC.sock <MANIFEST_CID>
cat peerC_export/foo.txt
```

To clean up the spawned log files, temp socket files, `export` directories, and abruptly kill all `p2pfs` processes:

```bash
./demo/rpc_demo.sh clean
```

## GCP Cross-VM Demo

### Terraform Setup

Terraform files for a simple 3-VM GCP demo environment live under `demo/gcp`.

The Terraform stack creates:

- `p2pfs-bootstrap`
- `p2pfs-peer-b`
- `p2pfs-peer-c`

along with a firewall rule that opens:

- `tcp:22` for SSH
- `tcp:4001-4010` for libp2p demo traffic

The Terraform stack is minimal:

- it creates the VMs
- it creates the network and firewall rules
- it does not install Go
- it does not clone the repo
- it does not build `p2pfs`

**Getting Started**
1. `cd demo/gcp`
2. `cp terraform.tfvars.example terraform.tfvars`
3. Fill in your GCP project and preferred zone.
4. `terraform init`
5. `terraform apply`
6. SSH into the three VMs with the `gcloud compute ssh ...` commands from Terraform outputs.
7. Install dependencies, clone the repo, build, and run the demo manually.

Don't forget to `terraform destroy` when you are done.

### Demo

**1. Start Nodes**

- Start the bootstrap node

```shell
mkdir -p ~/my_files
./p2pfs shell --listen /ip4/0.0.0.0/tcp/4001 --export_dir ~/my_files --name peerA

peerA> id
```

- Start peers

```shell
mkdir -p ~/my_files
./p2pfs shell --listen /ip4/0.0.0.0/tcp/4002 --export_dir ~/my_files --name peerB --bootstrap /ip4/<A_PUBLIC_IP>/tcp/4001/p2p/<A_PEER_ID>

peerB> id
```

```shell
./p2pfs shell --listen /ip4/0.0.0.0/tcp/4003 --export_dir ~/my_files --name peerC --bootstrap /ip4/<A_PUBLIC_IP>/tcp/4001/p2p/<A_PEER_ID>

peerC> id
peerC> alias peerB /ip4/<B_PUBLIC_IP>/tcp/4002/p2p/<B_PEER_ID>
```

**2. Create a New File in Peer B**

```shell
peerB> echo "hello from peer B" > foo.txt
peerB> dump 1000000000 > bar.txt
peerB> files
```

**3. Fetch from Peer C**

```shell
peerC> whohas <MANIFEST_CID>
peerC> fetch <MANIFEST_CID> peerB
peerC> cat ~/my_files/foo.txt
```
