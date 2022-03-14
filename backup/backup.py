"""
oVirt backup helpers.
"""

import glob
import json
import logging
import os
import ssl
import time

from contextlib import closing
from http import client as http_client
from urllib.parse import urlparse

from ovirt_imageio import client

import ovirtsdk4 as sdk
import ovirtsdk4.types as types

import imagetransfer

log = logging.getLogger("backup")


class Timeout:
    """
    Raised when starting or stopping a backup times out.
    """


def start_backup(connection, vm, from_checkpoint=None, backup_id=None,
                 description=None, timeout=300):
    log.info("Starting backup for vm %r from checkpoint %r",
             vm.name, from_checkpoint)

    start = time.monotonic()
    deadline = start + timeout

    vm_service = _get_vm_service(connection, vm)

    disk_attachments = vm_service.disk_attachments_service().list()
    disks = [connection.follow_link(a.disk) for a in disk_attachments]

    backups_service = vm_service.backups_service()
    backup = backups_service.add(
        types.Backup(
            id=backup_id,
            disks=disks,
            from_checkpoint_id=from_checkpoint,
            description=description,
        ),
        # Hack until we have a proper API model change.
        query={'with_snapshot': 'true'},
    )

    log.debug("Waiting until backup %r is ready", backup.id)

    backup_service = backups_service.backup_service(backup.id)

    while backup.phase != types.BackupPhase.READY:
        if time.monotonic() > deadline:
            raise Timeout(
                f"Timeout waiting until backup {backup.id} is ready")

        time.sleep(5)
        backup = _get_backup(backup_service, backup.id)

    log.info("Backup %r started in %.1f seconds",
             backup.id, time.monotonic() - start)

    return backup


def stop_backup(connection, backup, timeout=600):
    log.info("Stopping backup %r", backup.id)

    start = time.monotonic()
    deadline = start + timeout

    backup_service = _get_backup_service(connection, backup)

    backup_service.finalize()

    log.debug("Waiting until backup %r stops", backup.id)

    while backup.phase != types.BackupPhase.SUCCEEDED:
        if time.monotonic() > deadline:
            raise Timeout(
                f"Timeout waiting until backup {backup.id} stops")

        time.sleep(10)
        backup = _get_backup(backup_service, backup.id)

    log.info("Backup %r stopped in %.1f seconds",
             backup.id, time.monotonic() - start)


def download_backup(connection, backup, backup_dir, ca_file=None,
                    secure=False, verify=False):
    log.info("Downloading backup %r disks", backup.id)

    start = time.monotonic()

    backup_service = _get_backup_service(connection, backup)
    backup_disks = backup_service.disks_service().list()

    for disk in backup_disks:
        incremental = disk.backup_mode == types.DiskBackupMode.INCREMENTAL
        file_name = "{}.{}.{}.qcow2".format(
            backup.to_checkpoint_id, disk.id, disk.backup_mode)
        filename = os.path.join(backup_dir, file_name)

        if incremental:
            backing_file = _find_backing_file(
                backup_dir, backup.from_checkpoint_id, disk.id)
        else:
            backing_file = None

        _download_disk(
            connection, backup, disk, filename,
            incremental=incremental,
            backing_file=backing_file,
            ca_file=ca_file,
            secure=secure)

        if verify:
            _verify_backup(connection, backup, disk, filename, ca_file)

    log.info("Backup %r downloaded in %.1f seconds",
             backup.id, time.monotonic() - start)

    return filename


def _download_disk(connection, backup, disk, filename, incremental=False,
                   backing_file=None, ca_file=None, secure=False):
    backup_mode = "incremental" if incremental else "full"
    log.info("Downloading disk %r %s backup to file %r using backing file %r",
             disk.id, backup_mode, filename, backing_file)

    start = time.monotonic()

    transfer = imagetransfer.create_transfer(
        connection,
        disk,
        types.ImageTransferDirection.DOWNLOAD,
        backup=types.Backup(id=backup.id),
    )
    try:
        client.download(
            transfer.transfer_url,
            filename,
            ca_file,
            incremental=incremental,
            secure=secure,
            backing_file=backing_file,
            backing_format="qcow2",
        )
    finally:
        imagetransfer.finalize_transfer(connection, transfer, disk)

    log.info("Disk %r %s backup downloaded in %.1f seconds",
             disk.id, backup_mode, time.monotonic() - start)


def _verify_backup(connection, backup, disk, filename, ca_file):
    log.info("Verifying disk %r backup", disk.id)

    start = time.monotonic()

    expected = _disk_checksum(connection, backup, disk, ca_file)
    actual = _backup_checksum(filename, disk)
    if expected != actual:
        raise RuntimeError(
            f"Checksum mismatch: expected {expected} got {actual}")

    log.info("Disk %r backup verified in %.1f seconds",
             disk.id, time.monotonic() - start)

def _disk_checksum(connection, backup, disk, ca_file):
    log.info("Computing disk %r checksum", disk.id)

    start = time.monotonic()

    transfer = imagetransfer.create_transfer(
        connection,
        disk,
        types.ImageTransferDirection.DOWNLOAD,
        backup=types.Backup(id=backup.id),
    )
    try:
        url = urlparse(transfer.transfer_url)
        context = ssl.create_default_context(cafile=ca_file)

        con = http_client.HTTPSConnection(url.netloc, context=context)
        with closing(con):
            con.request("GET", url.path + "/checksum")
            res = con.getresponse()
            if res.status != http_client.OK:
                error = res.read().decode("utf-8", "replace")
                raise RuntimeError(f"Error computing checksum: {error}")

            result = json.loads(res.read())
    finally:
        imagetransfer.finalize_transfer(connection, transfer, disk)

    log.info("Disk %r checksum computed in %.1f seconds",
             disk.id, time.monotonic() - start)

    return result


def _backup_checksum(filename, disk):
    log.info("Computing disk %r backup checksum", disk.id)
    start = time.monotonic()
    result = client.checksum(filename)
    log.info("Disk %r backup checksum computed in %.1f seconds",
             disk.id, time.monotonic() - start)
    return result


# General helpers

def _get_vm_service(connection, vm):
    system_service = connection.system_service()
    return system_service.vms_service().vm_service(vm.id)


def _get_backup_service(connection, backup):
    vm_service = _get_vm_service(connection, backup.vm)
    return vm_service.backups_service().backup_service(backup.id)


def _get_backup(backup_service, backup_id):
    """
    Get backup, raising if backup does not exist or failed, so code monitoring
    backup phase does not have to repeat these checks.
    """
    try:
        backup = backup_service.get()
    except sdk.NotFoundError:
        raise RuntimeError(f"Backup {backup_id} does not exist")

    if backup.phase == types.BackupPhase.FAILED:
        raise RuntimeError(f"Backup {backup_id} has failed")

    return backup


def _find_backing_file(backup_dir, checkpoint_id, disk_id):
    """
    Return the name of the backing file for checkpoint, or None if the file was
    not found.
    """
    pattern = os.path.join(backup_dir, f"{checkpoint_id}.{disk_id}.*.qcow2")
    matches = glob.glob(pattern)
    if not matches:
        return None

    # The backing file can be an absolute path or a relative path from the
    # image directory. Using a relative path make is easier to manage.
    return os.path.relpath(matches[0], backup_dir)
