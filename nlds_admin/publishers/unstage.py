# encoding: utf-8
"""
unstage.py
"""

__author__ = "Neil Massey"
__date__ = "22 July 2026"
__copyright__ = "Copyright 2026 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from typing import Optional

from nlds_admin.rabbit.rpc_publisher import RabbitMQRPCPublisher
from nlds_admin.rabbit.publisher import RabbitMQPublisher
from nlds_admin.rabbit import message_keys as MSG
from nlds_admin.rabbit import routing_keys as RK
from nlds_admin.publishers.find import find_files
from nlds_admin.common.deserialize import deserialize
from nlds_admin.common.bcolors import bcolors
from nlds_admin.common.create_sub_id import create_sub_id
from nlds_admin.rabbit.state import State


def get_files_from_holding(
    rpc_publisher: RabbitMQRPCPublisher,
    user: str,
    group: str,
    holding_id: int,
    transaction_id: str,
    limit: int,
):

    # get the list of files using the holding_id or transaction_id
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
    print(bcolors.YELLOW + "  Unstaging files:" + bcolors.ENDC)
    for h in file_data[MSG.HOLDINGS]:
        holding = file_data[MSG.HOLDINGS][h]
        for tr in holding[MSG.TRANSACTIONS]:
            return_dict[tr] = []
            files = holding[MSG.TRANSACTIONS][tr][MSG.FILELIST]
            for f in files:
                print(f"    {f["original_path"]}")
                return_dict[tr].append(f["original_path"])
    return return_dict


def send_catalog_remove_message(
    rabbit_publisher: RabbitMQPublisher,
    user: str,
    group: str,
    holding_id: int,
    transaction_id: str,
    filelist: list[str],
):
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
            MSG.API_ACTION: "unstage",
            MSG.JOB_LABEL: "catalog-remove",
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
        },
        MSG.TYPE: MSG.TYPE_STANDARD,
    }
    # send the message and move onto the next transaction
    rabbit_publisher.publish_message(
        routing_key=routing_key,
        msg_dict=msg_dict,
    )


def unstage_holding(
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

    files_dict = get_files_from_holding(
        rpc_publisher=rpc_publisher,
        user=user,
        group=group,
        holding_id=holding_id,
        transaction_id=transaction_id,
        limit=limit,
    )

    rabbit_publisher = RabbitMQPublisher()
    rabbit_publisher.get_connection()

    for tr in files_dict:
        filelist = files_dict[tr]
        if len(filelist) > 0:
            send_catalog_remove_message(
                rabbit_publisher=rabbit_publisher,
                user=user,
                group=group,
                holding_id=holding_id,
                transaction_id=tr,
                filelist=filelist,
            )

    rabbit_publisher.close_connection()
