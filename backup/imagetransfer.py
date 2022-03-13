"""
oVirt image transfer helpers.
"""

import logging
import time

import ovirtsdk4 as sdk
from ovirtsdk4 import types

log = logging.getLogger("imagetransfer")


def find_host(connection, sd_name):
    """
    Check if we can preform a transfer using the local host and return a host
    instance. Return None if we cannot use this host.

    Using the local host for an image transfer allows optimizing the connection
    using unix socket. This speeds up the transfer significantly and minimizes
    the network bandwidth.

    However using the local host is possible only if:
    - The local host is a oVirt host
    - The host is Up
    - The host is in the same DC of the storage domain

    Consider this setup:

        laptop1

        dc1
            host1 (down)
            host2 (up)
            sd1
                disk1

        dc2
            host3 (up)
            sd2
                disk2

    - If we run on laptop1 we cannot use the local host for any transfer.
    - If we run on host1, we cannot use the local host for any transfer.
    - If we run on host2, we can use use host2 for transferring disk1.
    - If we run on host3, we can use host3 for transferring disk2.

    Arguments:
        connection (ovirtsdk4.Connection): Connection to ovirt engine
        sd_name (str): Storage domain name

    Returns:
        ovirtsdk4.types.Host
    """

    # Try to read this host hardware id.

    try:
        with open("/etc/vdsm/vdsm.id") as f:
            vdsm_id = f.readline().strip()
    except FileNotFoundError:
        log.debug("Not running on oVirt host, using any host")
        return None
    except OSError as e:
        # Unexpected error when running on ovirt host. Since choosing a host is
        # an optimization, log and continue.
        log.warning("Cannot read /etc/vdsm/vdsm.id, using any host: %s", e)
        return None

    log.debug("Found host hardware id: %s", vdsm_id)

    # Find the data center by storage domain name.

    system_service = connection.system_service()
    data_centers = system_service.data_centers_service().list(
        search='storage.name=%s' % sd_name,
        case_sensitive=True,
    )
    if len(data_centers) == 0:
        raise RuntimeError(
            "Storage domain {} is not attached to a DC"
            .format(sd_name))

    data_center = data_centers[0]
    log.debug("Found data center: %s", data_center.name)

    # Validate that this host is up and in data center.

    hosts_service = system_service.hosts_service()
    hosts = hosts_service.list(
        search="hw_id={} and datacenter={} and status=Up".format(
            vdsm_id, data_center.name),
        case_sensitive=True,
    )
    if len(hosts) == 0:
        log.debug(
            "Cannot use host with hardware id %s, host is not up, or does "
            "not belong to data center %s",
            vdsm_id, data_center.name)
        return None

    host = hosts[0]
    log.debug("Using host id %s", host.id)

    return host


def create_transfer(
        connection, disk=None, direction=types.ImageTransferDirection.UPLOAD,
        host=None, backup=None, inactivity_timeout=None, timeout=60,
        disk_snapshot=None, shallow=None,
        timeout_policy=types.ImageTransferTimeoutPolicy.LEGACY):
    """
    Create image transfer for upload to disk or download from disk.

    Arguments:
        connection (ovirtsdk4.Connection): connection to ovirt engine
        disk (ovirtsdk4.types.Disk): disk object. Not needed if disk_snaphost
            is specified.
        direction (ovirtsdk4.typles.ImageTransferDirection): transfer
            direction (default UPLOAD)
        host (ovirtsdk4.types.Host): host object that should perform the
            transfer. If not specified engine will pick a random host.
        backup (ovirtsdk4.types.Backup): When downloading backup, the backup
            object owning the disks.
        inactivity_timeout (int): Number of seconds engine will wait for client
            activity before pausing the transfer. If not set, use engine
            default value.
        timeout (float, optional): number of seconds to wait for transfer
            to become ready.
        disk_snapshot (ovirtsdk4.types.DiskSnapshot): transfer a disk snapshot
            instead of current data of the disk.
        shallow (bool): Download only the specified image instead of the entire
            image chain. When downloading a disk transfer only the active disk
            snapshot data. When downloading a disk snapshot, transfer only the
            specified disk snaphost data.
        timeout_policy (ovirtsdk4.types.ImageTransferTimeoutPolicy): the action
            to take after inactivity timeout.

    Returns:
        ovirtsdk4.types.ImageTransfer in phase TRANSFERRING
    """
    log.info(
        "Creating image transfer for %s=%s, direction=%s host=%s backup=%s "
        "shallow=%s timeout_policy=%s",
        "disk_snapshot" if disk_snapshot else "disk",
        disk_snapshot.id if disk_snapshot else disk.id,
        direction,
        host.name if host else None,
        backup.id if backup else None,
        shallow,
        timeout_policy,
    )

    start = time.monotonic()
    deadline = start + timeout

    transfer = types.ImageTransfer(
        host=host,
        direction=direction,
        backup=backup,
        inactivity_timeout=inactivity_timeout,
        timeout_policy=timeout_policy,
        format=types.DiskFormat.RAW,
        shallow=shallow,
    )

    if disk_snapshot:
        transfer.snapshot = types.DiskSnapshot(id=disk_snapshot.id)
    else:
        transfer.disk = types.Disk(id=disk.id)

    transfers_service = connection.system_service().image_transfers_service()

    transfer = transfers_service.add(transfer)

    # At this point the transfer owns the disk and will delete the disk if the
    # transfer is canceled, or if finalizing the transfer fails.

    transfer_service = transfers_service.image_transfer_service(transfer.id)

    while True:
        try:
            transfer = transfer_service.get()
        except sdk.NotFoundError:
            # The system has removed the disk and the transfer.
            raise RuntimeError(f"Transfer {transfer.id} was removed")

        if transfer.phase == types.ImageTransferPhase.FINISHED_FAILURE:
            # The system will remove the disk and the transfer soon.
            raise RuntimeError(f"Transfer {transfer.id} has failed")

        if transfer.phase == types.ImageTransferPhase.PAUSED_SYSTEM:
            transfer_service.cancel()
            raise RuntimeError(f"Transfer {transfer.id} was paused by system")

        if transfer.phase == types.ImageTransferPhase.TRANSFERRING:
            break

        if transfer.phase != types.ImageTransferPhase.INITIALIZING:
            transfer_service.cancel()
            raise RuntimeError(
                f"Unexpected transfer {transfer.id} phase {transfer.phase}")

        if time.monotonic() > deadline:
            log.info("Cancelling transfer %s", transfer.id)
            transfer_service.cancel()
            raise RuntimeError(
                f"Timed out waiting for transfer {transfer.id}")

        time.sleep(1)

    # Log the transfer host name. This is very useful for troubleshooting.
    transfer.host = connection.follow_link(transfer.host)

    log.info("Transfer %r ready on host %r in %.1f seconds",
             transfer.id, transfer.host.name, time.monotonic() - start)


    return transfer


def cancel_transfer(connection, transfer):
    """
    Cancel a transfer and remove the disk for upload transfer.

    There is not need to cancel a download transfer, it can always be
    finalized.
    """
    log.info("Cancelling transfer %r", transfer.id)
    transfer_service = (connection.system_service()
                            .image_transfers_service()
                            .image_transfer_service(transfer.id))
    transfer_service.cancel()


def finalize_transfer(connection, transfer, disk, timeout=300):
    """
    Finalize a transfer, making the transfer disk available.

    If finalizing succeeds: the disk status will change to OK and transfer's
    phase will change to FINISHED_SUCCESS.
    On upload errors: the disk status will change to ILLEGAL, transfer's phase
    will change to FINISHED_FAILURE and the disk will be removed.
    In both cases the transfer entity:
     a. prior to 4.4.7: removed shortly after the command finishes
     b. 4.4.7 and later: stays in the database and is cleaned by the dedicated
        thread after a few minutes.

    If oVirt fails to finalize the transfer, transfer's phase will change to
    PAUSED_SYSTEM. In this case the disk's status will change to ILLEGAL and it
    will not be removed.

    When working with oVirt 4.4.7 and later, it is enough to poll the image
    transfer. However with older versions the transfer entity is removed from
    the database after the the command finishes and before we can retrieve the
    final transfer status. Thus the API returns a 404 error code. In that case
    we need to check for the disk status.

    For more info see:
    - http://ovirt.github.io/ovirt-engine-api-model/4.4/#services/image_transfer
    - http://ovirt.github.io/ovirt-engine-sdk/master/types.m.html#ovirtsdk4.types.ImageTransfer

    Arguments:
        connection (ovirtsdk4.Connection): connection to ovirt engine
        transfer (ovirtsdk4.types.ImageTransfer): image transfer to finalize
        disk (ovirtsdk4.types.Disk): disk associated with the image transfer
        timeout (float, optional): number of seconds to wait for transfer
            to finalize.
    """
    log.info("Finalizing transfer %r for disk %r", transfer.id, disk.id)

    start = time.monotonic()

    transfer_service = (connection.system_service()
                            .image_transfers_service()
                            .image_transfer_service(transfer.id))

    transfer_service.finalize()
    while True:
        time.sleep(1)
        try:
            transfer = transfer_service.get()
        except sdk.NotFoundError:
            # Old engine (< 4.4.7): since the transfer was already deleted from
            # the database, we can assume that the disk status is already
            # updated, so we can check it only once.
            disk_service = (connection.system_service()
                                .disks_service()
                                .disk_service(disk.id))
            try:
                disk = disk_service.get()
            except sdk.NotFoundError:
                # Disk verification failed and the system removed the disk.
                raise RuntimeError(
                    f"Transfer {transfer.id} failed: disk {disk.id} was removed")

            if disk.status == types.DiskStatus.OK:
                break

            raise RuntimeError(
                f"Transfer {transfer.id} failed: disk {disk.id} status {disk.status}")

        log.debug("Transfer %r in phase %r", transfer.id, transfer.phase)

        if transfer.phase == types.ImageTransferPhase.FINISHED_SUCCESS:
            break

        if transfer.phase == types.ImageTransferPhase.FINISHED_FAILURE:
            raise RuntimeError(f"Transfer {transfer.id} failed")

        if time.monotonic() > start + timeout:
            raise RuntimeError(
                f"Timed out waiting for transfer {transfer.id} to finalize, "
                f"transfer is {transfer.phase}")

    log.info("Transfer %r finalized in %.1f seconds",
             transfer.id, time.monotonic() - start)
