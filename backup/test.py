import logging
import os
import shutil
import subprocess
import threading
import time
import uuid
import yaml

from collections import Counter

import ovirtsdk4 as sdk
import ovirtsdk4.types as types

from . import backup

log = logging.getLogger("test")


class Timeout(Exception):
    pass


class JobError(Exception):
    pass


class Runner:

    def __init__(self, index, conf):
        self.index = index
        self.conf = conf
        self.connection = None
        self.iteration = None

        # Runner stats.
        self.full_backups = 0
        self.incremental_backups = 0
        self.passed = 0
        self.failed = 0
        self.errored = 0

        # Test state.
        self.vm = None
        self.vm_address = None
        self.backup_dir = None

    def run(self):
        log.info("Started")
        self.connect()

        for i in range(self.conf["iterations"]):
            self.iteration = i
            start = time.monotonic()
            log.info("Iteration %d started", i)

            try:
                self.setup()
                try:
                    self.test()
                except Exception:
                    log.exception("Iteration %d failed", i)
                    self.failed += 1
            except Exception:
                log.exception("Iteration %d errored", i)
                self.errored += 1
            finally:
                try:
                    self.teardown()
                except Exception:
                    log.exception("Error tearing down")

            log.info("Iteration %d completed in %.1f seconds",
                     i, time.monotonic() - start)

        self.disconnect()
        log.info("Finished")

    def setup(self):
        self.check_data_center()
        self.create_vm()
        self.start_vm()
        self.vm_address = self.find_vm_address()
        self.create_backup_dir()

    def test(self):
        # Start full backup and stop the VM during the backup. This is
        # possible only when using hybrid backup.

        log.info("Running full backup for vm %s", self.vm.name)
        self.full_backups += 1

        full_backup = self.start_backup()
        try:
            self.write_data_in_guest(full_backup)
            self.stop_vm()
            vm_is_up = False
            self.download_backup(full_backup)
        finally:
            self.stop_backup(full_backup)

        self.passed += 1
        last_backup = full_backup

        # Run incremental backup, starting the VM during the backup.
        # This is possible only if using hybrid backup.

        for i in range(1, self.conf["incremental_backups"] + 1):
            log.info("Running incremental backup %d/%d for vm %s",
                     i, self.conf["incremental_backups"], self.vm.name)
            self.incremental_backups += 1

            incr_backup = self.start_backup(last_backup)
            try:
                if not vm_is_up:
                    self.start_vm()
                    vm_is_up = True

                if self.conf["delete_previous"]:
                    self.delete_data_in_guest(last_backup)
                self.write_data_in_guest(incr_backup)
                self.download_backup(incr_backup)
            finally:
                self.stop_backup(incr_backup)

            self.passed += 1
            last_backup = incr_backup

    def start_backup(self, last_backup=None):
        checkpoint = last_backup.to_checkpoint_id if last_backup else None
        return backup.start_backup(
            self.connection,
            self.vm,
            from_checkpoint=checkpoint,
            timeout=self.conf["start_backup_timeout"])

    def stop_backup(self, b):
        backup.stop_backup(
            self.connection,
            b,
            timeout=self.conf["stop_backup_timeout"])

    def write_data_in_guest(self, backup):
        log.info("Writing data in vm %s", self.vm.name)
        start = time.monotonic()

        write_method = self.conf["write_method"]
        if write_method == "zeroes":
            # The backup will sparsify the zeroes when writing to the backup
            # file, so incremental backups will be tiny.
            src = "/dev/zero"
        elif write_method == "random":
            # Every incremental will write random data to backup file.
            src = "/dev/urandom"
        else:
            raise RuntimeError(f"Usupported write method: {write_method!r}")

        count = self.conf["write_size_mb"]

        script = (
            f"dd if={src} bs=1M count={count} of=backup-{backup.id}.data "
            "oflag=direct conv=fsync"
        )
        self.run_in_guest(script)

        log.info("Wrote data in vm %s in %.1f seconds",
                 self.vm.name, time.monotonic() - start)

    def delete_data_in_guest(self, backup):
        log.info("Deleting data in vm %s", self.vm.name)
        start = time.monotonic()

        # The backup should create a zero cluster for the trimmed dirty
        # clusters without downloading the data.
        script = f"rm -f backup-{backup.id}.data; fsync; fstrim -av"
        self.run_in_guest(script)

        log.info("Deleted data in vm %s in %.1f seconds",
                 self.vm.name, time.monotonic() - start)

    def run_in_guest(self, script):
        cmd = [
            "ssh",
            # Disable host key verification, we trust our vm.
            "-o", "UserKnownHostsFile=/dev/null",
            "-o", "StrictHostKeyChecking=no",
            "-l", "root",
            self.vm_address,
            "bash", "-s",
        ]

        log.debug("Running command %s with input %r", cmd, script)

        r = subprocess.run(
            cmd,
            input=script.encode("utf-8"),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)

        if r.returncode != 0:
            log.warning("Command failed rc=%r out=%r err=%r",
                        r.returncode, r.stdout, r.stderr)
        else:
            log.debug("Command succeeded out=%r err=%r",
                      r.stdout, r.stderr)

    def download_backup(self, b):
        backup.download_backup(
            self.connection,
            b,
            self.backup_dir,
            ca_file=self.conf["engine_cafile"],
            secure=True,
            verify=True)

    def teardown(self):
        if self.vm:
            try:
                if not self.conf["keep_data"]:
                    self.remove_backup_dir()
                self.check_data_center()
                self.stop_vm()
                if not self.conf["keep_data"]:
                    self.remove_vm()
            finally:
                self.vm = None
                self.vm_address = None

    def connect(self):
        self.connection = sdk.Connection(
            url="https://{}/ovirt-engine/api".format(self.conf["engine_fqdn"]),
            username=self.conf["engine_username"],
            password=self.conf["engine_password"],
            ca_file=self.conf["engine_cafile"]
        )

    def disconnect(self):
        self.connection.close()

    # Data center health

    def check_data_center(self):
        log.info("Checking data center status for cluster %s",
                 self.conf["cluster_name"])

        start = time.monotonic()
        deadline = start + self.conf["data_center_up_timeout"]

        system_service = self.connection.system_service()

        clusters_service = system_service.clusters_service()
        clusters = clusters_service.list(
            search="name={}".format(self.conf["cluster_name"]))

        cluster = clusters[0]

        data_centers_service = system_service.data_centers_service()
        data_center_service = data_centers_service.data_center_service(
            id=cluster.data_center.id)

        data_center = data_center_service.get()
        log.debug("Data center %s is %s",
                  data_center.name, data_center.status)
        if data_center.status == types.DataCenterStatus.UP:
            return

        log.info("Data center %s is %s, waiting until it is up",
                 data_center.name, data_center.status)

        while True:
            time.sleep(self.conf["poll_interval"])
            data_center = data_center_service.get()
            log.debug("Data center %s is %s",
                      data_center.name, data_center.status)
            if data_center.status == types.DataCenterStatus.UP:
                break

            if time.monotonic() > deadline:
                raise Timeout(
                    "Timeout waiting until data center {} is up"
                    .format(data_center.name))

        log.info("Data center %s recovered in %.1f seconds",
                 data_center.name, time.monotonic() - start)

    # Modifying VMs.

    def create_vm(self):
        vm_name = "{}-{:02d}-{:02d}".format(
            self.conf["vm_name"], self.index, self.iteration)
        log.info("Creating vm %s", vm_name)

        start = time.monotonic()
        deadline = start + self.conf["create_vm_timeout"]

        vms_service = self.connection.system_service().vms_service()
        correlation_id = str(uuid.uuid4())

        self.vm = vms_service.add(
            types.Vm(
                name=vm_name,
                cluster=types.Cluster(name=self.conf["cluster_name"]),
                template=types.Template(name=self.conf["template_name"]),
            ),
            # Clone to VM to keep raw disks raw.
            clone=True,
            query={'correlation_id': correlation_id},
        )

        try:
            self.wait_for_jobs(correlation_id, deadline)
        except JobError:
            self.vm = None
            raise

        log.info("VM %s created in %.1f seconds",
                 self.vm.name, time.monotonic() - start)

    def start_vm(self):
        log.info("Starting vm %s", self.vm.name)

        start = time.monotonic()
        deadline = start + self.conf["start_vm_timeout"]

        vms_service = self.connection.system_service().vms_service()
        vm_service = vms_service.vm_service(self.vm.id)
        vm_service.start()

        self.wait_for_vm_status(types.VmStatus.UP, deadline)

        log.info("VM %s started in %.1f seconds",
                 self.vm.name, time.monotonic() - start)

    def stop_vm(self):
        log.info("Stopping vm %s", self.vm.name)

        start = time.monotonic()
        deadline = start + self.conf["stop_vm_timeout"]

        vms_service = self.connection.system_service().vms_service()
        vm_service = vms_service.vm_service(self.vm.id)

        vm = vm_service.get()

        if vm.status == types.VmStatus.IMAGE_LOCKED:
            # VM creation did not finish yet - create_vm() timed out.
            self.wait_for_vm_status(types.VmStatus.DOWN, deadline)
        elif vm.status != types.VmStatus.DOWN:
            self.try_to_stop_vm(deadline)

        log.info("VM %s stopped in %.1f seconds",
                 self.vm.name, time.monotonic() - start)

    def try_to_stop_vm(self, deadline):
        vms_service = self.connection.system_service().vms_service()
        vm_service = vms_service.vm_service(self.vm.id)

        # Testing shows that if a vm is in WAIT_FOR_LUNCH state,
        # stopping it does nothing. To handle all possible cases, lets
        # repeat the stop request and check the status until the VM is
        # DOWN or the timeout expires.

        while True:
            try:
                vm_service.stop()
            except sdk.Error as e:
                log.warning("Error stopping vm %s: %s", self.vm.name, e)

            time.sleep(self.conf["poll_interval"])
            vm = vm_service.get()
            log.debug("VM %s is %s", self.vm.name, vm.status)
            if vm.status == types.VmStatus.DOWN:
                break

            if time.monotonic() > deadline:
                raise Timeout(
                    "Timeout stopping vm {}".format(self.vm.name))

    def remove_vm(self):
        log.info("Removing vm %s", self.vm.name)

        start = time.monotonic()
        deadline = start + self.conf["remove_vm_timeout"]

        vms_service = self.connection.system_service().vms_service()
        vm_service = vms_service.vm_service(self.vm.id)

        vm_service.remove()

        while True:
            if time.monotonic() > deadline:
                raise Timeout("Timeout removing vm {}".format(self.vm.name))

            time.sleep(self.conf["poll_interval"])
            try:
                vm = vm_service.get()
            except sdk.NotFoundError:
                break
            except sdk.Error as e:
                log.warning("Error polling vm %s status, retrying: %s",
                            self.vm.name, e)
                continue

            log.debug("VM %s status: %s", self.vm.name, vm.status)

        log.info("VM %s removed in %.1f seconds",
                 self.vm.name, time.monotonic() - start)
        self.vm = None

    def wait_for_vm_status(self, status, deadline):
        log.debug("Waiting until vm %s is %s", self.vm.name, status)

        vms_service = self.connection.system_service().vms_service()
        vm_service = vms_service.vm_service(self.vm.id)

        while True:
            if time.monotonic() > deadline:
                raise Timeout(
                    "Timeout waiting until vm {} is {}"
                    .format(self.vm.name, status))

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
                                    "Found vm %s address %r in %.1f seconds",
                                    self.vm.name, ip.address, time.monotonic() - start)
                                return ip.address

            time.sleep(self.conf["poll_interval"])

    # Polling jobs.

    def wait_for_jobs(self, correlation_id, deadline):
        log.debug("Waiting for jobs with correlation id %s", correlation_id)

        while not self.jobs_completed(correlation_id):
            time.sleep(self.conf["poll_interval"])
            if time.monotonic() > deadline:
                raise Timeout(
                    "Timeout waiting for jobs with correlation id {}"
                    .format(correlation_id))

    def jobs_completed(self, correlation_id):
        """
        Return True if all jobs with specified correlation id have completed,
        False otherwise.

        Raise JobError if some jobs have failed or aborted.
        """
        jobs_service = self.connection.system_service().jobs_service()

        try:
            jobs = jobs_service.list(
                search="correlation_id={}".format(correlation_id))
        except sdk.Error as e:
            log.warning(
                "Error searching for jobs with correlation id %s: %s",
                correlation_id, e)
            # We dont know, assume that jobs did not complete yet.
            return False

        if all(job.status != types.JobStatus.STARTED for job in jobs):
            # In some cases like create snapshot, it is not be possible to
            # detect the failure by checking the entity.
            failed_jobs = [(job.description, str(job.status))
                           for job in jobs
                           if job.status != types.JobStatus.FINISHED]
            if failed_jobs:
                raise JobError(
                    "Some jobs for with correlation id {} have failed: {}"
                    .format(correlation_id, failed_jobs))

            return True
        else:
            jobs_status = [(job.description, str(job.status)) for job in jobs]
            log.debug("Some jobs with correlation id %s are running: %s",
                      correlation_id, jobs_status)
            return False

    # Backup.

    def create_backup_dir(self):
        backup_dir = os.path.join(self.conf["backup_dir"], self.vm.name)
        log.debug("Creatinng backup directory %r", backup_dir)
        os.makedirs(backup_dir)
        self.backup_dir = backup_dir

    def remove_backup_dir(self):
        if self.backup_dir:
            log.debug("Removing backup directory %r", self.backup_dir)
            try:
                shutil.rmtree(self.backup_dir)
            except Exception:
                log.exception("Error removing %r, manual cleanup needed",
                              self.backup_dir)
            self.backup_dir = None


with open("conf.yml") as f:
    conf = yaml.safe_load(f)

logging.basicConfig(
    level=logging.DEBUG if conf["debug"] else logging.INFO,
    format="%(asctime)s %(levelname)-7s (%(threadName)s) %(message)s")

start = time.monotonic()
stats = Counter()
runners = []

for i in range(conf["vms_count"]):
    name = "run/{:02d}".format(i)
    log.info("Starting runner %s", name)
    r = Runner(i, conf)
    t = threading.Thread(target=r.run, name=name, daemon=True)
    t.start()
    runners.append((r, t))

    log.debug("Waiting %.1f seconds before starting next runner", conf["run_delay"])
    time.sleep(conf["run_delay"])

try:
    for _, t in runners:
        log.debug("Waiting for runner %s", t.name)
        t.join()
except KeyboardInterrupt:
    log.info("Test interrupted, aborting test without cleanup")

for r, _ in runners:
    stats["full_backups"] += r.full_backups
    stats["incremental_backups"] += r.incremental_backups
    stats["passed"] += r.passed
    stats["failed"] += r.failed
    stats["errored"] += r.errored

log.info("%d full backups, %d incremental backups, %d failed, %d passed, "
         "%d errored in %.1f minutes",
         stats["full_backups"],
         stats["incremental_backups"],
         stats["failed"],
         stats["passed"],
         stats["errored"],
         (time.monotonic() - start) / 60)
