import argparse
import logging
import subprocess
import threading
import time
import yaml

import ovirtsdk4 as sdk
import ovirtsdk4.types as types

log = logging.getLogger("test")


class Timeout(Exception):
    pass


class Runner:

    def __init__(self, index, conf):
        self.index = index
        self.conf = conf
        self.connection = None
        self.vm = None
        self.snapshot = None

    def run(self):
        log.info("Started")
        self.connect()

        for i in range(self.conf["iterations"]):
            start = time.monotonic()
            log.info("Iteration %d started", i)
            try:
                self.create_vm()
                self.start_vm()
                self.create_snapshot()
                self.write_data()
                self.remove_snapshot()
            except Exception:
                log.exception("Iteration %d failed", i)
            else:
                log.info("Iteration %d succeeded in %d seconds",
                         i, time.monotonic() - start)
            finally:
                if self.vm:
                    self.stop_vm()
                    self.remove_vm()

        self.disconnect()
        log.info("Finished")

    def connect(self):
        self.connection = sdk.Connection(
            url="https://{}/ovirt-engine/api".format(self.conf["engine_fqdn"]),
            username=self.conf["engine_username"],
            password=self.conf["engine_password"],
            ca_file=self.conf["engine_cafile"]
        )

    def disconnect(self):
        self.connection.close()

    def create_vm(self):
        vm_name = "{}-{}".format(self.conf["vm_name"], self.index)
        log.info("Creating vm %s", vm_name)

        start = time.monotonic()

        vms_service = self.connection.system_service().vms_service()

        self.vm = vms_service.add(
            types.Vm(
                name=vm_name,
                cluster=types.Cluster(name=self.conf["cluster_name"]),
                template=types.Template(name=self.conf["template_name"]),
                placement_policy=types.VmPlacementPolicy(
                    hosts=[
                        types.Host(name=self.conf["vm_host"])
                    ]
                ),
            ),
            # Clone to VM to keep raw disks raw.
            clone=True,
        )

        self.wait_for_vm_status(
            types.VmStatus.DOWN, self.conf["create_vm_timeout"])

        log.info("VM %s created in %d seconds",
                 self.vm.name, time.monotonic() - start)

    def start_vm(self):
        log.info("Starting vm %s", self.vm.name)

        start = time.monotonic()

        vms_service = self.connection.system_service().vms_service()
        vm_service = vms_service.vm_service(self.vm.id)
        vm_service.start()

        self.wait_for_vm_status(
            types.VmStatus.UP, self.conf["start_vm_timeout"])

        log.info("VM %s started in %d seconds",
                 self.vm.name, time.monotonic() - start)

    def stop_vm(self):
        log.info("Stopping vm %s", self.vm.name)

        start = time.monotonic()

        vms_service = self.connection.system_service().vms_service()
        vm_service = vms_service.vm_service(self.vm.id)

        for i in range(10):
            try:
                vm_service.stop()
            except sdk.Error as e:
                log.warning("Error stopping vm, retrying: %s", e)
                time.sleep(self.conf["poll_interval"])
            else:
                break

        try:
            self.wait_for_vm_status(
                types.VmStatus.DOWN, self.conf["stop_vm_timeout"])
        except sdk.NotFoundError:
            log.warning("VM %s not found: %s", self.vm.name)
        else:
            log.info("VM %s stopped in %d seconds",
                     self.vm.name, time.monotonic() - start)

    def remove_vm(self):
        log.info("Removing vm %s", self.vm.name)

        start = time.monotonic()

        vms_service = self.connection.system_service().vms_service()
        vm_service = vms_service.vm_service(self.vm.id)

        for i in range(10):
            try:
                vm_service.remove()
            except sdk.Error as e:
                log.warning("Error removing vm, retrying: %s", e)
                time.sleep(self.conf["poll_interval"])
            else:
                break

        while True:
            time.sleep(self.conf["poll_interval"])
            try:
                vm = vm_service.get()
            except sdk.NotFoundError:
                break
            except sdk.Error as e:
                log.warning("Error polling vm, retrying: %s", e)
                continue

            log.debug("VM %s status: %s", self.vm.name, vm.status)

        log.info("VM %s removed in %d seconds",
                 self.vm.name, time.monotonic() - start)
        self.vm = None

    def wait_for_vm_status(self, status, timeout):
        log.debug("Waiting up to %d seconds until vm %s is %s",
                  timeout, self.vm.name, status)

        deadline = time.monotonic() + timeout

        vms_service = self.connection.system_service().vms_service()
        vm_service = vms_service.vm_service(self.vm.id)

        while True:
            if time.monotonic() > deadline:
                raise Timeout("Timeout waiting until vm is {}".format(status))

            time.sleep(self.conf["poll_interval"])
            try:
                vm = vm_service.get()
            except sdk.NotFoundError:
                # Adding vm failed.
                self.vm = None
                raise
            except sdk.Error as e:
                log.warning("Error polling vm, retrying: %s", e)
                continue

            if vm.status == status:
                break

            log.debug("VM %s status: %s", self.vm.name, vm.status)

    def create_snapshot(self):
        log.info("Creating snapshot for vm %s", self.vm.name)

        start = time.monotonic()

        vms_service = self.connection.system_service().vms_service()
        vm_service = vms_service.vm_service(self.vm.id)
        snapshots_service = vm_service.snapshots_service()

        self.snapshot = snapshots_service.add(
            types.Snapshot(
                description='Snapshot 1',
                persist_memorystate=False
            )
        )

        self.wait_for_snapshot_status(
            types.SnapshotStatus.OK, self.conf["create_snapshot_timeout"])

        log.info("Created snapshot %s for vm %s in %d seconds",
                 self.snapshot.id, self.vm.name, time.monotonic() - start)

    def remove_snapshot(self):
        log.info("Removing snapshot %s for vm %s",
                 self.snapshot.id, self.vm.name)

        start = time.monotonic()
        deadline = start + self.conf["remove_snapshot_timeout"]

        vms_service = self.connection.system_service().vms_service()
        vm_service = vms_service.vm_service(self.vm.id)
        snapshots_service = vm_service.snapshots_service()
        snapshot_service = snapshots_service.snapshot_service(self.snapshot.id)

        for i in range(10):
            try:
                snapshot_service.remove()
            except sdk.Error as e:
                log.warning("Error removing snapshot, retrying: %s", e)
                time.sleep(self.conf["poll_interval"])
            else:
                break

        while True:
            if time.monotonic() > deadline:
                raise Timeout(
                    "Timeout waiting until snapshot {} is removed"
                    .format(self.snapshot.id))

            time.sleep(self.conf["poll_interval"])
            try:
                snapshot = snapshot_service.get()
            except sdk.NotFoundError:
                break
            except sdk.Error as e:
                log.warning("Error polling snapshot, retrying: %s", e)
                continue

            log.debug("Snapshot %s status: %s",
                      self.snapshot.id, snapshot.snapshot_status)

        log.info("Removed snapshot %s for vm %s in %d seconds",
                 self.snapshot.id, self.vm.name, time.monotonic() - start)
        self.snapshot = None

    def wait_for_snapshot_status(self, status, timeout):
        log.debug("Waiting up to %d seconds until snapshot %s is %s",
                  timeout, self.snapshot.id, status)

        deadline = time.monotonic() + timeout

        vms_service = self.connection.system_service().vms_service()
        vm_service = vms_service.vm_service(self.vm.id)
        snapshots_service = vm_service.snapshots_service()
        snapshot_service = snapshots_service.snapshot_service(self.snapshot.id)

        while True:
            if time.monotonic() > deadline:
                raise Timeout(
                    "Timeout waiting until snapshot is {}".format(status))

            time.sleep(self.conf["poll_interval"])
            try:
                snapshot = snapshot_service.get()
            except sdk.NotFoundError:
                # Adding snapshot failed.
                self.snapshot = None
                raise
            except sdk.Error as e:
                log.warning("Error polling snapshot, retrying: %s", e)
                continue

            if snapshot.snapshot_status == status:
                break

            log.debug("Snapshot %s status: %s",
                      self.snapshot.id, snapshot.snapshot_status)

    def write_data(self):
        try:
            vm_address = self.find_vm_address()
        except Timeout:
            log.warning("Timeout finding vm %s address, skipping write data",
                        self.vm.name)
            return

        log.info("Writing data in vm %s address %s", self.vm.name, vm_address)

        script = b"""
            for disk in /dev/vd*; do
                dd if=/dev/zero bs=1M count=2048 \
                   of=$disk oflag=direct conv=fsync &
            done
            wait
        """

        cmd = [
            "ssh",
            # Disable host key verification, we trust the vm we just created.
            "-o", "UserKnownHostsFile=/dev/null",
            "-o", "StrictHostKeyChecking=no",
            "-l", "root",
            vm_address,
            "bash", "-s",
        ]

        log.debug("Running command: %s", cmd)

        r = subprocess.run(
            cmd,
            input=script,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)

        if r.returncode != 0:
            log.warning("Command failed rc=%r out=%r err=%r",
                        r.returncode, r.stdout, r.stderr)
        else:
            log.debug("Command succeeded out=%r err=%r",
                      r.stdout, r.stderr)

    def find_vm_address(self):
        start = time.monotonic()
        deadline = start + self.conf["vm_address_timeout"]

        vms_service = self.connection.system_service().vms_service()
        vm_service = vms_service.vm_service(self.vm.id)
        nics_service = vm_service.nics_service()

        while True:
            if time.monotonic() > deadline:
                raise Timeout(
                    "Timeout waiting for vm {} address".format(self.vm.name))

            for nic in nics_service.list():
                nic_service = nics_service.nic_service(nic.id)
                devices_service = nic_service.reported_devices_service()
                for device in devices_service.list():
                    device_service = devices_service.reported_device_service(device.id)
                    device = device_service.get()
                    # Sometiems device.ips is None.
                    if device.ips:
                        for ip in device.ips:
                            if ip.version == types.IpVersion.V4:
                                log.info(
                                    "Found vm %s address in %d seconds",
                                    self.vm.name, time.monotonic() - start)
                                return ip.address

            time.sleep(self.conf["poll_interval"])


with open("conf.yml") as f:
    conf = yaml.safe_load(f)

logging.basicConfig(
    level=logging.DEBUG if conf["debug"] else logging.INFO,
    format="%(asctime)s %(levelname)-7s (%(threadName)s) %(message)s")

threads = []

for i in range(conf["vms_count"]):
    name = "run/{}".format(i)
    log.info("Starting runner %s", name)
    r = Runner(i, conf)
    t = threading.Thread(target=r.run, name=name, daemon=True)
    t.start()
    threads.append(t)

    log.info("Waiting %d seconds before starting next runner", conf["run_delay"])
    time.sleep(conf["run_delay"])

for t in threads:
    log.info("Waiting for runner %s", t.name)
    t.join()

log.info("Finished")
