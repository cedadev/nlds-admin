# encoding: utf-8
"""
status.py
"""
__author__ = "Neil Massey"
__date__ = "24 Feb 2025"
__copyright__ = "Copyright 2025 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

from typing import Optional, Union
import uuid
import json

import rabbit.message_keys as MSG
import rabbit.routing_keys as RK
from rabbit.state import State

from rabbit.rpc_publisher import RabbitMQRPCPublisher

def get_request_status(
    rpc_publisher: RabbitMQRPCPublisher,        
    user: str,
    group: str,
    groupall: Optional[bool] = False,
    id: Optional[int] = None,
    transaction_id: Optional[str] = None,
    job_label: Optional[str] = None,
    state: Optional[Union[int, str]] = None,
    sub_id: Optional[str] = None,
    api_action: Optional[str] = None
):
    # create the message dictionary
    api_action = f"{RK.STAT}"

    # Validate state at this point.
    if state is not None:
        # Attempt to convert to int, if can't then put in upper case for name
        # comparison
        try:
            state = int(state)
        except (ValueError, TypeError):
            state = state.upper()

        if State.has_name(state):
            state = State[state].value
        elif State.has_value(state):
            state = State(state).value
        else:
            msg="Given State not valid."
            raise Exception(msg)

    # Validate transaction_id is a valid uuid
    if transaction_id is not None:
        try:
            uuid.UUID(transaction_id)
        except ValueError:
            msg="Given transaction_id not a valid uuid-4."
            raise Exception(msg)
    # Validate sub_id is a valid uuid
    if sub_id is not None:
        try:
            uuid.UUID(sub_id)
        except ValueError:
            msg="Given sub_id not a valid uuid-4."
            raise Exception(msg)

    # Assemble message ready for RCP call
    msg_dict = {
        MSG.DETAILS: {
            MSG.USER: user,
            MSG.GROUP: group,
            MSG.GROUPALL: groupall,
            MSG.ID: id,
            MSG.API_ACTION: api_action,
            MSG.TRANSACT_ID: transaction_id,
            MSG.JOB_LABEL: job_label,
            MSG.STATE: state,
            MSG.SUB_ID: sub_id,
            MSG.USER_QUERY: user,
            MSG.GROUP_QUERY: group,
            MSG.API_ACTION: api_action,
        },
        MSG.DATA: {},
        MSG.TYPE: MSG.TYPE_STANDARD,
    }

    # call RPC function
    routing_key = "monitor_q"
    response = rpc_publisher.call(msg_dict=msg_dict, routing_key=routing_key)
    # Check if response is valid or whether t   he request timed out
    if response is not None:
        # convert byte response to dict for label fetching
        response_dict = json.loads(response)
        # Attempt to get list of transaction records
        transaction_records = None
        try:
            transaction_records = response_dict[MSG.DATA][MSG.RECORD_LIST]
        except KeyError as e:
            msg = (f"Encountered error when trying to get a record list from the"
                   f" message response ({e})")
            raise Exception(msg)

        transaction_response = None
        # Only continue if the response actually had any transactions in it
        if transaction_records is not None and len(transaction_records) > 0:
            routing_key = "catalog_q"
            transaction_response = rpc_publisher.call(
                msg_dict=response_dict, routing_key=routing_key
            )

        if transaction_response is not None:
            response = transaction_response

        # convert byte response to str
        response = response.decode()
        return response
    else:
        msg="Monitoring service could not be reached in time.",
        raise Exception(msg)
