# encoding: utf-8
"""
fix_tape_records.py
"""

__author__ = "Neil Massey"
__date__ = "09 July 2026"
__copyright__ = "Copyright 2026 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from typing import Optional
from nlds_admin.rabbit.rpc_publisher import RabbitMQRPCPublisher
from nlds_admin.rabbit.publisher import RabbitMQPublisher
from nlds_admin.rabbit import message_keys as MSG
from nlds_admin.rabbit import routing_keys as RK
from nlds_admin.rabbit.state import State
from nlds_admin.publishers.find import find_files
from nlds_admin.common.deserialize import deserialize
from nlds_admin.common.bcolors import bcolors
from nlds_admin.common.create_sub_id import create_sub_id


def file_has_empty_tape_storage_location(file: dict):
    locations = file["locations"]
    for l in locations:
        if l["storage_type"] == "TAPE" and l["root"] == "" and l["url"] == "":
            return True
    return False


def get_files_with_incomplete_records(
    rpc_publisher: RabbitMQRPCPublisher,
    user: str,
    group: str,
    holding_id: int,
    transaction_id: str,
    limit: int,
) -> dict:
    """Use the find_files function to get a list of files for the holding or
    transaction.  Then filter these on whether they have a complete TAPE record.
    If root is the null string, and url is also the null string then the TAPE record
    is not valid.
    It returns a dictionary of:
    {
        "transaction_id": ["original_path_1", "original_path_2"]
    }
    which can then be used to send a message to CATALOG_REMOVE.
    """
    # First of all get a list of the files for the holding or transaction_id
    files_stat = find_files(
        rpc_publisher=rpc_publisher,
        user=user,
        group=group,
        holding_id=holding_id,
        transaction_id=transaction_id,
        limit=limit,
    )
    # Requires deserializing (decompressing)
    files_response = deserialize(files_stat)
    print(
        bcolors.YELLOW
        + "Working on Holding with holding_id:\n"
        + bcolors.ENDC
        + f"  {files_response[MSG.DETAILS][MSG.HOLDING_ID]}"
    )
    file_data = files_response[MSG.DATA]
    return_dict = {}
    print(bcolors.YELLOW + "  Untaping files:" + bcolors.ENDC)
    for h in file_data[MSG.HOLDINGS]:
        holding = file_data[MSG.HOLDINGS][h]
        for tr in holding[MSG.TRANSACTIONS]:
            return_dict[tr] = []
            files = holding[MSG.TRANSACTIONS][tr][MSG.FILELIST]
            for f in files:
                if file_has_empty_tape_storage_location(f):
                    print(f"    {f["original_path"]}")
                    return_dict[tr].append(f["original_path"])
    return return_dict


def send_archive_remove_message(
    rabbit_publisher: RabbitMQPublisher,
    user: str,
    group: str,
    holding_id: int,
    transaction_id: str,
    filelist: list[str],
):
    """Remove records of the files Tape locations by sending an archive_remove
    message"""
    # create the rabbit publisher and build the routing_key
    routing_key = f"{RK.ROOT}.{RK.CATALOG_REMOVE}.{RK.START}"
    # build the message: it contains the filelist converted to file_details
    sub_id = str(create_sub_id(filelist=filelist))
    msg_filelist = []
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
                "holding_id": holding_id,
            },
        }
        msg_filelist.append(fj)

    # Build the message to contain the filelist
    msg_dict = {
        MSG.DETAILS: {
            MSG.TRANSACT_ID: transaction_id,
            MSG.SUB_ID: sub_id,
            MSG.API_ACTION: "archive-put",
            MSG.JOB_LABEL: "archive-remove",
            MSG.USER: user,
            MSG.GROUP: group,
            MSG.STATE: State.CATALOG_REMOVING.value,
            MSG.ROUTE: "NLDS_ADMIN",
        },
        MSG.DATA: {
            MSG.FILELIST: msg_filelist,
        },
        MSG.META: {
            MSG.HOLDING_ID: holding_id,
            MSG.STORAGE_TYPE: "TAPE",
        },
        MSG.TYPE: MSG.TYPE_STANDARD,
    }
    # send the message and move onto the next transaction
    rabbit_publisher.publish_message(
        routing_key=routing_key,
        msg_dict=msg_dict,
    )


def fix_holding_tape_records(
    rpc_publisher: RabbitMQRPCPublisher,
    user: str,
    group: str,
    holding_id: int,
    transaction_id: Optional[str] = None,
    limit=1000,
    json: Optional[bool] = False,
):
    # error check - need to supply user, group, id and / or transaction
    # need user and group
    if not user or not group:
        raise RuntimeError("User and group are required to perform a fix-status.")

    # need one of holding id or transaction id
    if not (holding_id or transaction_id):
        raise RuntimeError(
            "One of the following is required to perform a fix-status: holding_id, "
            "transaction_id."
        )

    incomplete_files = get_files_with_incomplete_records(
        rpc_publisher=rpc_publisher,
        user=user,
        group=group,
        holding_id=holding_id,
        transaction_id=transaction_id,
        limit=limit,
    )

    if len(incomplete_files) > 0:
        n_incmpl_files = 0
        # count the incomplete files for each transaction in the dictionary
        for k in incomplete_files:
            n_incmpl_files += len(incomplete_files[k])
        print(
            bcolors.YELLOW
            + f"    Number of incomplete files: {n_incmpl_files}"
            + bcolors.ENDC
        )

        rabbit_publisher = RabbitMQPublisher()
        rabbit_publisher.get_connection()

        # Send a message for each transaction in the catalog_remove_dict
        for tr in incomplete_files:
            filelist = incomplete_files[tr]
            send_archive_remove_message(
                rabbit_publisher=rabbit_publisher,
                user=user,
                group=group,
                holding_id=holding_id,
                transaction_id=tr,
                filelist=filelist,
            )

        rabbit_publisher.close_connection()
