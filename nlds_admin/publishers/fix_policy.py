from typing import Optional
import click

import nlds_admin.rabbit.routing_keys as RK
import nlds_admin.rabbit.message_keys as MSG

from nlds_admin.rabbit.rpc_publisher import RabbitMQRPCPublisher
from nlds_admin.common.deserialize import deserialize
from nlds_admin.common.connect import ObjectStoreConnection
from nlds_admin.publishers.view_buckets import (
    make_list_message,
    get_bucket_name,
    decode_policy,
)


def fix_bucket_policy(
    rpc_publisher: RabbitMQRPCPublisher,
    user: Optional[str] = "",
    group: Optional[str] = "",
    holding_id: Optional[int] = None,
    transaction_id: Optional[str] = None,
    limit: Optional[int] = None,
    json: Optional[bool] = False,
):
    # connect to object store
    s3_client = ObjectStoreConnection()

    msg_dict = make_list_message(user, group, holding_id, transaction_id, limit)

    # call RPC function
    routing_key = f"{RK.CATALOG_Q}"
    response = rpc_publisher.call(msg_dict=msg_dict, routing_key=routing_key)
    # Check if response is valid or whether the request timed out
    if response is not None:
        # convert byte response to str
        response = response.decode()
        json_response = deserialize(response)
        holdings = json_response[MSG.DATA][MSG.HOLDINGS]
        for h in holdings:
            click.echo(f"{'':<1}{h['user']:<16}{h['group']:<16}{h['id']:<6}")
            for t in h[MSG.TRANSACTIONS]:
                # if we want to view the policy then we have to fetch it from the
                # object store
                policy = s3_client.get_bucket_policy(get_bucket_name(t))
                click.echo(f"{'':<5}{get_bucket_name(t)} {decode_policy(policy)}")
    else:
        msg = "Catalog service could not be reached in time."
        raise RuntimeError(msg)
