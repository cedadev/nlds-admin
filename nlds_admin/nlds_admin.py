import click

from nlds_admin.rabbit.rpc_publisher import RabbitMQRPCPublisher
from nlds_admin.publishers.list import list_holdings
from nlds_admin.publishers.find import find_files
from nlds_admin.publishers.status import get_request_status

from nlds_admin import prints
from nlds_admin.deserialize import deserialize

@click.group()
@click.pass_context
def nlds_admin(ctx):
    rpc_publisher = RabbitMQRPCPublisher()
    rpc_publisher.get_connection()
    ctx.obj = rpc_publisher


@nlds_admin.command("list", help="List holdings.")
@click.pass_context
@click.option(
    "-u", "--user", default=None, type=str, help="The username to list holdings for."
)
@click.option(
    "-g", "--group", default=None, type=str, help="The group to list holdings for."
)
@click.option(
    "-A",
    "--groupall",
    default=False,
    is_flag=True,
    help="List holdings that belong to a group, rather than a single user",
)
@click.option(
    "-l",
    "--label",
    default=None,
    type=str,
    help="The label of the holding(s) to list.  This can be a regular"
    "expression (regex).",
)
@click.option(
    "-i",
    "--holding_id",
    default=None,
    type=int,
    help="The numeric id of the holding to list.",
)
@click.option(
    "-n",
    "--transaction_id",
    default=None,
    type=str,
    help="The UUID transaction id of the transaction to list.",
)
@click.option(
    "-t",
    "--tag",
    default=None,
    type=str,
    help="The tag(s) of the holding(s) to list.",
)
@click.option(
    "-j", "--json", default=False, is_flag=True, help="Output the result as JSON."
)
@click.option(
    "-L", "--limit", default=None, type=int, help="Limit the number of holdings to list"
)
@click.option(
    "-9/-0",
    "--descending/--ascending",
    "time",
    default=False,
    help="Switch between ascending and descending time order.",
)
def list(
    ctx,
    user,
    group,
    groupall,
    label,
    holding_id,
    transaction_id,
    tag,
    json,
    limit,
    time,
):
    rpc_publisher = ctx.obj
    try:
        ret = list_holdings(
            rpc_publisher=rpc_publisher,
            user="nlds",
            group="",
            groupall=groupall,
            label=label,
            holding_id=holding_id,
            transaction_id=transaction_id,
            tag=tag,
            query_user=user,
            query_group=group,
            limit=limit,
            time=time,
        )
    finally:
        rpc_publisher.close_connection()
    json_response = deserialize(ret)
    response_details = json_response["details"]
    if "meta" in json_response:
        response_meta = json_response["meta"]
    else:
        response_meta = {}

    if "failure" in response_details and len(response_details["failure"]) > 0:
        fail_string = "Failed to list holdings "
        fail_string += prints.construct_header_string(
            response_details, response_meta, time
        )
        if response_details["failure"]:
            fail_string += "\n" + response_details["failure"]
        raise click.UsageError(fail_string)

    response_data = json_response["data"]["holdings"]

    if json:
        click.echo(json_response)
    else:
        prints.print_action(response_data, response_details, response_meta, time)


@nlds_admin.command("find", help="Find and list files.")
@click.pass_context
@click.option(
    "-u", "--user", default=None, type=str, help="The username to find files for."
)
@click.option(
    "-g", "--group", default=None, type=str, help="The group to find files for."
)
@click.option(
    "-A",
    "--groupall",
    default=False,
    is_flag=True,
    help="Find files that belong to a group, rather than a single user",
)
@click.option(
    "-l",
    "--label",
    default=None,
    type=str,
    help="The label of the holding which the files belong to.  This "
    "can be a regular expression (regex).",
)
@click.option(
    "-i",
    "--holding_id",
    default=None,
    type=int,
    help="The numeric id of the holding which the files belong to.",
)
@click.option(
    "-n",
    "--transaction_id",
    default=None,
    type=str,
    help="The UUID transaction id of the transaction to list.",
)
@click.option(
    "-p",
    "--path",
    default=None,
    type=str,
    help="The path of the files to find.  This can be a regular expression (regex)",
)
@click.option(
    "-t",
    "--tag",
    default=None,
    type=str,
    help="The tag(s) of the holding(s) to find files within.",
)
@click.option(
    "-j",
    "--json",
    default=False,
    type=bool,
    is_flag=True,
    help="Output the result as JSON.",
)
@click.option(
    "-1",
    "--simple",
    default=False,
    type=bool,
    is_flag=True,
    help="Output the list of files, one per line, filepath only.",
)
@click.option(
    "-U",
    "--url",
    default=False,
    type=bool,
    is_flag=True,
    help="Output the URL for the file on the object storage.",
)
@click.option(
    "-L", "--limit", default=None, type=int, help="Limit the number of files to list"
)
@click.option(
    "-9/-0",
    "--descending/--ascending",
    "time",
    default=False,
    help="Switch between ascending and descending time order.",
)
def find(
    ctx,
    user,
    group,
    groupall,
    label,
    holding_id,
    transaction_id,
    path,
    tag,
    json,
    simple,
    url,
    limit,
    time,
):
    rpc_publisher = ctx.obj
    try:
        ret = find_files(
            rpc_publisher=rpc_publisher,
            user="nlds",
            group="",
            groupall=groupall,
            label=label,
            holding_id=holding_id,
            transaction_id=transaction_id,
            path=path,
            tag=tag,
            query_user=user,
            query_group=group,
            limit=limit,
            descending=time,
        )
    finally:
        rpc_publisher.close_connection()
    json_response = deserialize(ret)
    response_details = json_response["details"]
    if "meta" in json_response:
        response_meta = json_response["meta"]
    else:
        response_meta = {}

    if "failure" in response_details and len(response_details["failure"]) > 0:
        fail_string = "Failed to find files "
        fail_string += prints.construct_header_string(
            response_details, response_meta, time, simple, url
        )
        if response_details["failure"]:
            fail_string += "\n" + response_details["failure"]
        raise click.UsageError(fail_string)

    response_data = json_response["data"]["holdings"]

    if json:
        click.echo(json_response)
    else:
        prints.print_action(
            response_data, response_details, response_meta, time, simple, url
        )


@nlds_admin.command("stat", help="List transactions.")
@click.pass_context
@click.option(
    "-u",
    "--user",
    default=None,
    type=str,
    help="The username to list transactions for.",
)
@click.option(
    "-g", "--group", default=None, type=str, help="The group to list transactions for."
)
@click.option(
    "-A",
    "--groupall",
    default=False,
    is_flag=True,
    help="List transactions that belong to a group, rather than a single user",
)
@click.option(
    "-i",
    "--id",
    default=None,
    type=int,
    help="The numeric id of the transaction to list.",
)
@click.option(
    "-n",
    "--transaction_id",
    default=None,
    type=str,
    help="The UUID transaction id of the transaction to list.",
)
@click.option(
    "-b",
    "--job_label",
    default=None,
    type=str,
    help="The job label of the transaction(s) to list.",
)
@click.option(
    "-s",
    "--state",
    default=None,
    type=str,
    help="The state of the transactions to list.  Options: "
    "INITIALISING | ROUTING | SPLITTING | INDEXING | "
    "CATALOG_PUTTING | TRANSFER_PUTTING | CATLOG_ROLLBACK | "
    "CATALOG_GETTING | ARCHIVE_GETTING | TRANSFER_GETTING | "
    "ARCHIVE_INIT | CATALOG_ARCHIVE_AGGREGATING | ARCHIVE_PUTTING | "
    "CATALOG_ARCHIVE_UPDATING | CATALOG_ARCHIVE_ROLLBACK | "
    "COMPLETE | FAILED | COMPLETE_WITH_ERRORS | "
    "COMPLETE_WITH_WARNINGS",
)
@click.option(
    "--sub_id",
    default=None,
    help="transactions that have been split due to size.",
)
@click.option(
    "-a",
    "--api_action",
    default=None,
    type=str,
    help="The api action of the transactions to list. Options: get | "
    "put | getlist | putlist",
)
@click.option(
    "-j",
    "--json",
    default=False,
    type=bool,
    is_flag=True,
    help="Output the result as JSON.",
)
@click.option(
    "-L",
    "--limit",
    default=None,
    type=int,
    help="Limit the number of transactions to list",
)
@click.option(
    "-9/-0",
    "--descending/--ascending",
    "time",
    default=True,
    help="Switch between ascending and descending time order.",
)
def stat(
    ctx,
    user,
    group,
    groupall,
    id,
    transaction_id,
    job_label,
    state,
    sub_id,
    api_action,
    json,
    limit,
    time,
):
    rpc_publisher = ctx.obj
    try:
        ret = get_request_status(
            rpc_publisher=rpc_publisher,
            user="nlds",
            group="",
            groupall=groupall,
            id=id,
            transaction_id=transaction_id,
            job_label=job_label,
            state=state,
            sub_id=sub_id,
            api_action=api_action,
            query_user=user,
            query_group=group,
            limit=limit,
            descending=time,
        )
    finally:
        rpc_publisher.close_connection()
    json_response = deserialize(ret)
    response_details = json_response["details"]
    response_data = json_response["data"]
    response_meta = json_response["meta"]
    response_data = response_data["records"]

    if json:
        click.echo(json_response)
    else:
        prints.print_action(response_data, response_details, response_meta, time)


def main():
    nlds_admin(prog_name="nlds-admin")


if __name__ == "__main__":
    nlds_admin()
