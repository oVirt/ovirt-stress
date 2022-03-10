# Stress test for incremental backup

## Overview

This test performs concurrent full and incremental backup.

## Physical environment

The test can run with one host, but it is recommended to test on at
least 2 hosts so we can test backup on the SPM host and on regular host.

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

The cluster name for creating the template test VMs. This cluster must
contain the selected host.

    cluster_name: "cluster-name"

### Controlling test size and duration

By default, the test creates 8 test VMs. On a smaller environment,
testing with lower number of VMs may be needed.

    vms_count: 8

Specify the number of test iterations for every VM. The default is good
for verifying that everything works. After that you can configure this
based on the time you want to run the test. On our test system one
iteration takes about 5 minutes.

    iterations: 1

Specify the number of incremental backups per iteration. For each
iteration, we perform a full backup, and then the specified number of
incremental backups. During each backup we write 1g of zeroes in the VM,
and delete the data created by the previous backup. The default is good
for verifying that everything works. After that you can configure this
based on the time you want to run the test.

    incremental_backups: 1

### Troubleshooting

If something goes wrong, you can enable very verbose logs to debug the
test.

    debug: true

## Creating a test template

Before running the test, you need to create the test template once.

Please run:

    ansible-playbook setup.yml

This creates a template named "backup-template". The test VMs
will be created from this template.

## Running the test

To run the tests please run:

    python3 test.py

To save the test log use:

    python3 test.py 2>test.log
