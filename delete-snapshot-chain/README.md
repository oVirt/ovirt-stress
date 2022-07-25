# Stress test for delete snapshot

## Overview

This stress test was created for testing the fix to bug
https://bugzilla.redhat.com/2103582.

The main purpose of the test is to create many snapshots in a single
VM so that they form a volume chain, and then delete them in order
so that we force volume chain to be synced and parents tags to be
updated after every removal.

Finally, delete the VM, so this stress other parts of the system as well,
and end the test in a clean state.

## Physical environment

The test can run with one host, but in real environment one host is
serving as the SPM, and VMs are most likely running on another host in
the cluster, so 2 hosts are recommended.

One host will server as SPM, and the other host will be used to run the
test VMs.

## Creating a test configuration

Please create a file named "conf.yml" by copying "conf.yml.example" and
modifying the variables.

### Engine connection

    engine_fqdn: "engine-fqdn"
    engine_username: "admin@internal"
    engine_password: "engine-password"
    engine_cafile: "/etc/pki/vdsm/certs/cacert.pem"

The default ``engine_cafile`` is correct when you run this test on one
of the hosts.

### Storage configuration

This is the template used to create the test VM template. This template
must have a public key of the host running the test. If needed, create a
public key with empty passphrase the host using ssh-keygen. This public
key is used by the test to connect to the test VM, and write data to the
disks.

    template_base: "template-base"

The storage domain name for creating the template and the test VMs.

    storage_domain: "storage-domain"

### Cluster configuration

The oVirt host name that the VM should run on. To simulate real world
flows, this host should not be the SPM host. This host must be in the
selected cluster.

    vm_host: "host-name"

The cluster name for creating the template test VMs. This cluster must
contain the selected host.

    cluster_name: "cluster-name"

### Controlling test size and duration

The test creates 1 test VM, creating multiple snapshots and then deleting
snapshots in order. Testing different number of snapshots is supported.
The default is a minimum scenario with 3 merges, two internal merges and
one active merge:

    num_snapshots: 3

We can also specify the number of test iterations. The default is good
for verifying that everything works. After that you can configure this
base on the time you want to run the test. Note that for bigger samples is
better to increase the number of snapshots rather than a large number of 
iterations. Creating and deleting a VM per iteration is slow, and deleting
more snapshots per iteration has the same verification value.
On our test system one iteration takes about 7 minutes, so 50 iterations 
will complete in 5 and a half hours.

    iterations: 1

### Troubleshooting

If something goes wrong, you can enable verbose logs to debug the test.

    debug: true

## Creating a test template

Before running the test, you need to create the test template once.

Please run:

    ansible-playbook setup.yml

This creates a template named "delete-snapshot-template". The test VMs
will be created from this template.

## Running the test

To run the tests please run:

    python3 test.py

To save the test log use:

    python3 test.py 2>/path/to/test.log
