# encoding: utf-8
"""
fix_status.py
"""

__author__ = "Neil Massey"
__date__ = "01 June 2026"
__copyright__ = "Copyright 2026 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from typing import Optional
from nlds_admin.rabbit.rpc_publisher import RabbitMQRPCPublisher
from nlds_admin.publishers.status import get_request_status
from nlds_admin.publishers.find import find_files
from nlds_admin.common.deserialize import deserialize
from nlds_admin.rabbit import routing_keys as RK
from nlds_admin.rabbit import message_keys as MSG
from nlds_admin.rabbit.state import State
from nlds_admin.common.connect import connect_to_object_store
from nlds_admin.rabbit.publisher import RabbitMQPublisher
from nlds_admin.common.bcolors import bcolors
import nlds_admin.common.config as CFG
from nlds_admin.common.create_sub_id import create_sub_id


def file_has_object_storage_location(file: dict) -> bool:
    storage_locations = file["locations"]
    # quick exit on no storage locations
    if len(storage_locations) == 0:
        return False

    # now check that one is OBJECT STORAGE or TAPE
    for l in storage_locations:
        if l["storage_type"] == "OBJECT_STORAGE":
            return True
        if l["storage_type"] == "TAPE":
            continue
    return False


def get_incomplete_sub_ids(
    rpc_publisher: RabbitMQRPCPublisher,
    user: str,
    group: str,
    state: str,
    id: int,
    transaction_id: str,
) -> tuple[list[str], str]:
    # first get a json representation of the requested transaction
    trans_stat = get_request_status(
        rpc_publisher=rpc_publisher,
        user=user,
        group=group,
        id=id,
        transaction_id=transaction_id,
    )
    # requires deserializing (decompressing)
    trans_response = deserialize(trans_stat)
    # get the transaction records
    trans_records = trans_response[MSG.DATA][MSG.RECORD_LIST]
    # loop over the record list and the sub records below
    incomplete_sub_ids = []
    if len(trans_records) > 0:
        transaction_id = trans_records[0][MSG.TRANSACT_ID]
        api_action = trans_records[0][MSG.API_ACTION]
    else:
        transaction_id = None
        api_action = None

    for tr in trans_records:
        print(
            bcolors.YELLOW
            + "Working on TransactionRecord:\n"
            + bcolors.ENDC
            + f"    {tr[MSG.TRANSACT_ID]}"
        )
        sub_records = tr[MSG.SUB_RECORD_LIST]
        for sr in sub_records:
            sr_state = sr[MSG.STATE]
            if sr_state == state:
                # need to do something with these sub records
                incomplete_sub_ids.append(sr[MSG.SUB_ID])
    # we need the transaction id for the get_incomplete_files
    return incomplete_sub_ids, transaction_id, api_action


def get_complete_and_incomplete_files(
    rpc_publisher: RabbitMQRPCPublisher,
    user: str,
    group: str,
    transaction_id: str,
) -> list[str]:
    # now get the files for the transaction - we need to use the transaction id
    # this may be None if the numeric id was used
    trans_files = find_files(
        rpc_publisher=rpc_publisher,
        user=user,
        group=group,
        transaction_id=transaction_id,
    )
    incomplete_files = []
    complete_files = []
    file_response = deserialize(trans_files)
    # there is a holding, then a transaction in the DATA section
    file_data = file_response[MSG.DATA]
    for h in file_data[MSG.HOLDINGS]:
        holding = file_data[MSG.HOLDINGS][h]
        for tr in holding[MSG.TRANSACTIONS]:
            if tr == transaction_id:
                files = holding[MSG.TRANSACTIONS][tr][MSG.FILELIST]
                for f in files:
                    # check the file has a storage location
                    if file_has_object_storage_location(f):
                        complete_files.append(f["original_path"])
                    else:
                        incomplete_files.append(f["original_path"])
    return complete_files, incomplete_files


def check_for_files_on_object_store(
    transaction_id: str,
    search_files: list[str],
) -> tuple[list[str], list[str]]:
    mc = connect_to_object_store()
    uploaded_files = []
    missing_files = []
    # nlds bucket is "nlds."+transaction_id
    bucket_name = "nlds." + transaction_id
    # stat the object to see if it exists
    result = mc.list_objects(bucket_name, recursive=True)
    uploaded_files = [r.object_name for r in result]
    set_of_uploaded = set(uploaded_files)
    set_of_search = set(search_files)
    # Get the list of files that is not on object store
    missing_files = list(set_of_search - set_of_uploaded)
    return uploaded_files, missing_files


def send_monitor_complete_message(
    rabbit_publisher: RabbitMQPublisher,
    user: str,
    group: str,
    transaction_id: str,
    sub_id: str,
    api_action: str,
) -> None:
    msg_dict = {
        MSG.DETAILS: {
            MSG.TRANSACT_ID: transaction_id,
            # for the root message, the sub_id is the transaction_id
            MSG.SUB_ID: sub_id,
            MSG.API_ACTION: api_action,
            MSG.JOB_LABEL: "monitor-complete",
            MSG.USER: user,
            MSG.GROUP: group,
            MSG.STATE: State.COMPLETE.value,
            MSG.ROUTE: "NLDS_ADMIN",
        },
        MSG.DATA: {
            MSG.FILELIST: [],
        },
        MSG.META: {
            # Insert an empty meta dict
        },
        MSG.TYPE: MSG.TYPE_STANDARD,
    }
    routing_key = f"{RK.ROOT}.{RK.MONITOR_PUT}.{RK.START}"
    rabbit_publisher.publish_message(
        routing_key=routing_key,
        msg_dict=msg_dict,
    )


def send_catalog_update_message(
    rabbit_publisher: RabbitMQPublisher,
    user: str,
    group: str,
    transaction_id: str,
    sub_id: str,
    api_action: str,
    filelist: list[str],
) -> None:
    """This sends an update message to the catalog for the files that have been
    uploaded to the object storage but not flagged as such in the database.
    The message requires the tenancy, bucket and original path for each file.
    The tenancy is in the config.
    The bucket is nlds.+the transaction id.
    The original path is in the filelist"""
    config = CFG.load_config()
    tenancy = config["cronjob_publisher"]["tenancy"]
    bucket = "nlds." + transaction_id

    # build the dictionary of the files locations
    json_filelist = []
    for f in filelist:
        fj = {
            "file_details": {
                "original_path": f,
                "path_type": 0,  # zero is FILE type
                "link_path": None,
                "size": 0,
                "user": 0,
                "group": 0,
                "permissions": 0,
                "mode": 0,
                "access_time": 0.0,
                "failure_reason": None,
                "holding_id": 0,
            },
            "storage_locations": {
                "OBJECT_STORAGE": {
                    "storage_type": "OBJECT_STORAGE",
                    "url_scheme": "http",
                    "url_netloc": tenancy,
                    "root": bucket,
                    "path": f,
                    "access_time": 0.0,
                }
            },
        }
        json_filelist.append(fj)

    msg_dict = {
        MSG.DETAILS: {
            MSG.TRANSACT_ID: transaction_id,
            # for the root message, the sub_id is the transaction_id
            MSG.SUB_ID: sub_id,
            MSG.API_ACTION: api_action,
            MSG.JOB_LABEL: "catalog-update",
            MSG.USER: user,
            MSG.GROUP: group,
            # be integer - can remove in future version when new server rolled out
            MSG.STATE: State.CATALOG_UPDATING.value,
            MSG.ROUTE: "NLDS_ADMIN",
        },
        MSG.DATA: {
            MSG.FILELIST: json_filelist,
        },
        MSG.META: {
            # Insert an empty meta dict
        },
        MSG.TYPE: MSG.TYPE_STANDARD,
    }

    routing_key = f"{RK.ROOT}.{RK.CATALOG_UPDATE}.{RK.START}"
    rabbit_publisher.publish_message(
        routing_key=routing_key,
        msg_dict=msg_dict,
    )


def send_catalog_delete_message(
    rabbit_publisher: RabbitMQPublisher,
    user: str,
    group: str,
    transaction_id: str,
    sub_id: str,
    api_action: str,
    filelist: list[str],
):
    json_filelist = []
    for f in filelist:
        failure_reason = f"Object with path {f} could not be found on object store."
        fj = {
            "file_details": {
                "original_path": f,
                "path_type": 0,  # zero is FILE type
                "link_path": None,
                "size": 0,
                "user": 0,
                "group": 0,
                "permissions": 0,
                "mode": 0,
                "access_time": 0.0,
                "failure_reason": failure_reason,
                "holding_id": 0,
            }
        }
        json_filelist.append(fj)

    msg_dict = {
        MSG.DETAILS: {
            MSG.TRANSACT_ID: transaction_id,
            # for the root message, the sub_id is the transaction_id
            MSG.SUB_ID: sub_id,
            MSG.API_ACTION: api_action,
            MSG.JOB_LABEL: "catalog-delete",
            MSG.USER: user,  # user is used in _catalog_del, and can be a string
            MSG.GROUP: group,  # group is used in _catalog_del, and can be a string
            MSG.STATE: State.CATALOG_DELETING.value,
            MSG.ROUTE: "NLDS_ADMIN",
        },
        MSG.DATA: {
            MSG.FILELIST: json_filelist,
        },
        MSG.META: {
            # Insert an empty meta dict
        },
        MSG.TYPE: MSG.TYPE_STANDARD,
    }
    # send catalog message
    catalog_routing_key = f"{RK.ROOT}.{RK.CATALOG_DEL}.{RK.START}"
    rabbit_publisher.publish_message(
        routing_key=catalog_routing_key,
        msg_dict=msg_dict,
    )


def fix_transfer_putting(
    rpc_publisher: RabbitMQRPCPublisher,
    rabbit_publisher: RabbitMQPublisher,
    user: str,
    group: str,
    transaction_id: list[str,],
    incomplete_sub_ids: list[str],
    api_action: str,
) -> None:
    """
    Fix the status of files that have errored in transfer.
    Also check that they are actually on the object store before committing any changes.
    Four cases:
    1.  The files are marked as complete (have object store record) and are present on
        the object store - just send a complete message to the monitor but do nothing
        to the database [complete_files]
    2.  The files are marked as complete but do not exist on the object store - set as
        incomplete and remove [missing files]
    3.  The files are not marked as complete but exist on the object store - set as
        complete and update the catalog [incomplete files]
    4.  The files are not marked as complete and do not exist on the object store - set
        as incomplete and remove [missing files]
    """
    # get the files that are incomplete
    complete_files, incomplete_files = get_complete_and_incomplete_files(
        rpc_publisher=rpc_publisher,
        user=user,
        group=group,
        transaction_id=transaction_id,
    )
    # check whether the "complete_files" are on the object store
    _, missing_complete_files = check_for_files_on_object_store(
        transaction_id,
        complete_files,
    )
    # remove missing_files from the complete files
    for m in missing_complete_files:
        complete_files.remove(m)

    # check whether the "incomplete_files" are on the object store
    _, missing_incomplete_files = check_for_files_on_object_store(
        transaction_id,
        incomplete_files,
    )
    # remove missing_files from the incomplete files
    for m in missing_incomplete_files:
        incomplete_files.remove(m)
    # munge both missing lists together
    missing_files = missing_complete_files
    missing_files.extend(missing_incomplete_files)

    # do incomplete files first
    if len(incomplete_files) != 0:
        # these files are incomplete - they are present on the object storage, but do
        # not have the correct entry in the database
        print(
            bcolors.GREEN
            + "    Incomplete files, on object store but not updated in database:"
            + bcolors.ENDC
        )
        for f in incomplete_files:
            print(f"        {f}")
        print(
            bcolors.RED
            + "Files have been found that are uploaded to the object store but do "
            "not have complete database records.\n"
            "Do you wish to fix them: Y/N ?" + bcolors.ENDC
        )
        user_response = input().lower()
        if user_response == "y":
            send_catalog_update_message(
                rabbit_publisher=rabbit_publisher,
                user=user,
                group=group,
                transaction_id=transaction_id,
                # use the first sub id for the monitoring - complete the other
                # sub records below
                sub_id=str(create_sub_id(filelist=incomplete_files)),
                api_action=api_action,
                filelist=incomplete_files,
            )

    if len(missing_files) > 0:
        print(
            bcolors.GREEN
            + "    Missing files, in database but not on object store:"
            + bcolors.ENDC
        )
        for f in missing_files:
            print(f"        {f}")
        print(
            bcolors.RED
            + "Files have been found that have database entries, but are missing "
            "from the object store. \n"
            "Do you wish to mark them as a failed upload: Y/N ?" + bcolors.ENDC
        )
        user_response = input().lower()
        if user_response == "y":
            send_catalog_delete_message(
                rabbit_publisher=rabbit_publisher,
                user=user,
                group=group,
                transaction_id=transaction_id,
                # use the first sub id for the monitoring - complete the other
                # sub records below
                sub_id=str(create_sub_id(filelist=missing_files)),
                api_action=api_action,
                filelist=missing_files,
            )

    # no files are incomplete, we can mark the sub ids as finished
    # need to send a message to the Monitor Queue
    print(
        bcolors.GREEN
        + "    Sub ids with all files uploaded to object store:"
        + bcolors.ENDC
    )
    for sid in incomplete_sub_ids:
        print(f"        {sid}")
    if len(incomplete_sub_ids) > 0:
        print(
            bcolors.RED
            + "Sub ids have been found with incomplete database records, even "
            "though the transfer completed.\n"
            "Do you wish to fix them: Y/N ?" + bcolors.ENDC
        )
        user_response = input().lower()
        if user_response == "y":
            # we start at an index offset if the complete and incomplete logic ran, so
            # that we do not send COMPLETE messages to those that are FAILED or
            # CATALOG_UPDATING
            for sid in incomplete_sub_ids:
                send_monitor_complete_message(
                    rabbit_publisher=rabbit_publisher,
                    user=user,
                    group=group,
                    transaction_id=transaction_id,
                    sub_id=sid,
                    api_action=api_action,
                )


def fix_transaction_status(
    rpc_publisher: RabbitMQRPCPublisher,
    user: str,
    group: str,
    state: str,
    id: Optional[int] = None,
    transaction_id: Optional[str] = None,
    json: bool = False,
):
    # error check - need to supply user, group, id and / or transaction
    # need user and group
    if not user or not group:
        raise RuntimeError("User and group are required to perform a fix-status.")
    if not state:
        raise RuntimeError("State is required to perform a fix-status.")

    # need one of id, transaction id or job_label
    if not (id or transaction_id):
        raise RuntimeError(
            "One of the following is required to perform a fix-status: "
            "--id (-i), --transaction_id (-n)"
        )

    incomplete_sub_ids, ret_trans_id, api_action = get_incomplete_sub_ids(
        rpc_publisher=rpc_publisher,
        user=user,
        group=group,
        state=state,
        id=id,
        transaction_id=transaction_id,
    )
    # override the transaction_id with the returned one if it is None
    # (if it is not None then they should be equal anyway)
    if transaction_id is None:
        transaction_id = ret_trans_id

    if len(incomplete_sub_ids) > 0:
        print(bcolors.YELLOW + "    Incomplete sub_ids" + bcolors.ENDC)
        for i in incomplete_sub_ids:
            print(f"        {i}")

        # branch on the state to do the required fix
        # only one at the moment (TRANSFER_PUTTING) but can extend this
        match state:
            case "TRANSFER_PUTTING":
                rabbit_publisher = RabbitMQPublisher()
                rabbit_publisher.get_connection()

                fix_transfer_putting(
                    rpc_publisher=rpc_publisher,
                    rabbit_publisher=rabbit_publisher,
                    user=user,
                    group=group,
                    transaction_id=transaction_id,
                    incomplete_sub_ids=incomplete_sub_ids,
                    api_action=api_action,
                )
                rabbit_publisher.close_connection()

            case "CATALOG_PUTTING":
                # I think this is the same as fix_transfer_putting
                rabbit_publisher = RabbitMQPublisher()
                rabbit_publisher.get_connection()
                fix_transfer_putting(
                    rpc_publisher=rpc_publisher,
                    rabbit_publisher=rabbit_publisher,
                    user=user,
                    group=group,
                    transaction_id=transaction_id,
                    incomplete_sub_ids=incomplete_sub_ids,
                    api_action=api_action,
                )
                rabbit_publisher.close_connection()
