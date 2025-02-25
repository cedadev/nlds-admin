
"""
list.py
"""
__author__ = "Neil Massey and Jack Leland"
__date__ = "24 Feb 2025"
__copyright__ = "Copyright 2025 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from typing import Optional

import rabbit.routing_keys as RK
import rabbit.message_keys as MSG
from publishers.process_tag import process_tag

from rabbit.rpc_publisher import RabbitMQRPCPublisher

def list_holdings(
    rpc_publisher: RabbitMQRPCPublisher,
    user: str,
    group: str,
    groupall: Optional[bool] = False,
    label:  Optional[str] = None,
    holding_id:  Optional[int] = None,
    transaction_id:  Optional[str] = None,
    tag:  Optional[str] = None,
):
    # create the message dictionary
    api_action = f"{RK.LIST}"
    msg_dict = {
        MSG.DETAILS: {
            MSG.USER: user,
            MSG.GROUP: group,
            MSG.GROUPALL: groupall,
            MSG.API_ACTION: api_action,
        },
        MSG.DATA: {},
        MSG.TYPE: MSG.TYPE_STANDARD,
    }
    # add the metadata
    meta_dict = {}
    if label:
        meta_dict[MSG.LABEL] = label
    if holding_id:
        meta_dict[MSG.HOLDING_ID] = holding_id
    if transaction_id:
        meta_dict[MSG.TRANSACT_ID] = transaction_id

    if tag:
        tag_dict = {}
        # convert the string into a dictionary
        try:
            tag_dict = process_tag(tag)
        except ValueError as e:
            raise ValueError(e)
        else:
            meta_dict[MSG.TAG] = tag_dict
    if len(meta_dict) > 0:
        msg_dict[MSG.META] = meta_dict

    # call RPC function
    routing_key = f"{RK.CATALOG_Q}"
    response = rpc_publisher.call(msg_dict=msg_dict, routing_key=routing_key)
    # Check if response is valid or whether the request timed out
    if response is not None:
        # convert byte response to str
        response = response.decode()
        return response
    else:
        msg="Catalog service could not be reached in time."
        raise RuntimeError(msg)
