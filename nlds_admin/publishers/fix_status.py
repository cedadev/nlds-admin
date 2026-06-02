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
            return True
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
        print(f"Working on TransactionRecord:\n    {tr[MSG.TRANSACT_ID]}")
        sub_records = tr[MSG.SUB_RECORD_LIST]
        for sr in sub_records:
            sr_state = sr[MSG.STATE]
            if sr_state == state:
                # need to do something with these sub records
                incomplete_sub_ids.append(sr[MSG.SUB_ID])
    # we need the transaction id for the get_incomplete_files
    return incomplete_sub_ids, transaction_id, api_action


def get_incomplete_files(
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
                    if not file_has_object_storage_location(f):
                        incomplete_files.append(f["original_path"])
    return incomplete_files


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
    missing_files = list(set_of_search.difference(set_of_uploaded))
    return uploaded_files, missing_files


def send_complete_message(
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
            MSG.JOB_LABEL: "monitor-put",
            MSG.USER: user,
            MSG.GROUP: group,
            MSG.STATE: State.COMPLETE.value,
            MSG.ROUTE: "",
        },
        MSG.DATA: {
            # Convert to PathDetails for JSON serialisation
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


# self.publish_message(monitoring_rk, body_json)


def fix_transfer_putting(
    rpc_publisher: RabbitMQRPCPublisher,
    rabbit_publisher: RabbitMQPublisher,
    user: str,
    group: str,
    transaction_id: list[str,],
    incomplete_sub_ids: list[str],
    api_action: str,
) -> None:
    # get the files that are incomplete
    incomplete_files = get_incomplete_files(
        rpc_publisher=rpc_publisher,
        user=user,
        group=group,
        transaction_id=transaction_id,
    )

    if len(incomplete_files) == 0:
        # no files are incomplete, we can mark the sub ids as finished
        # need to send a message to the Monitor Queue
        print("    Sub ids with all files uploaded to object store:")
        for sid in incomplete_sub_ids:
            print(f"        {sid}")
            send_complete_message(
                rabbit_publisher=rabbit_publisher,
                user=user,
                group=group,
                transaction_id=transaction_id,
                sub_id=sid,
                api_action=api_action,
            )
    else:
        # these files are incomplete
        # we should now check to see if they are present on the object storage
        # 1. if they are then we create a catalog update message with the files and
        #    the object storage location in the message
        # 2. if they are not then we create a remove from catalog message
        uploaded_files, missing_files = check_for_files_on_object_store(
            transaction_id,
            incomplete_files,
        )
        print("    Uploaded files, on object store but not updated in database:")
        for f in uploaded_files:
            print(f"        {f}")
        print("    Missing files, in database but not on object store:")
        for f in missing_files:
            print(f"        {f}")


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
        print("    Incomplete sub_ids")
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
