import click
from datetime import datetime

from nlds_admin.rabbit.rpc_publisher import RabbitMQRPCPublisher
from nlds_admin.publishers.list import list_holdings
from nlds_admin.publishers.find import find_files
from nlds_admin.publishers.status import get_request_status

import json as jsn

from nlds_admin import prints


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
    "-d/-a",
    "--descending/--ascending",
    "time",
    default=False,
    help="Switch between ascending and descending time order.",
)
def list(
    ctx, user, group, groupall, label, holding_id, transaction_id, tag, json, time
):
    if not (group and (user or groupall)):
        raise click.UsageError(
            "Could not list holdings: user and group must be supplied, or group plus "
            "`groupall` flag."
        )
    rpc_publisher = ctx.obj
    try:
        ret = list_holdings(
            rpc_publisher,
            user,
            group,
            groupall,
            label,
            holding_id,
            transaction_id,
            tag,
        )
    finally:
        rpc_publisher.close_connection()
    json_response = jsn.loads(ret)
    response_details = json_response["details"]
    response_data = json_response["data"]["holdings"]

    response_data = sorted(
        response_data,
        key=lambda x: datetime.strptime(x["date"], "%Y-%m-%dT%H:%M:%S.%f"),
        reverse=time,
    )

    if json:
        click.echo(json_response)
    else:
        prints.print_action(response_data, response_details, time)


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
    "-d/-a",
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
    time,
):
    if not (group and (user or groupall)):
        raise click.UsageError(
            "Could not find files: user and group must be supplied, or group plus "
            "`groupall` flag."
        )

    rpc_publisher = ctx.obj
    try:
        ret = find_files(
            rpc_publisher,
            user,
            group,
            groupall,
            label,
            holding_id,
            transaction_id,
            path,
            tag,
        )
    finally:
        rpc_publisher.close_connection()
    json_response = jsn.loads(ret)
    response_details = json_response["details"]
    response_data = json_response["data"]["holdings"]

    if response_data:
        response_data = dict(
            sorted(
                response_data.items(),
                key=lambda x: datetime.strptime(
                    x[1]["transactions"][next(iter(x[1]["transactions"]))][
                        "ingest_time"
                    ],
                    "%Y-%m-%dT%H:%M:%S.%f",
                ),
                reverse=time,
            )
        )

    if json:
        click.echo(json_response)
    else:
        prints.print_action(response_data, response_details, time, simple, url)


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
    "-d/-a",
    "--descending/--ascending",
    "time",
    default=False,
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
    time,
):
    if not (group and (user or groupall)):
        raise click.UsageError(
            "Could not stat requests: user and group must be supplied, or group plus "
            "`groupall` flag."
        )
    rpc_publisher = ctx.obj
    try:
        ret = get_request_status(
            rpc_publisher,
            user,
            group,
            groupall,
            id,
            transaction_id,
            job_label,
            state,
            sub_id,
            api_action,
        )
    finally:
        rpc_publisher.close_connection()
    json_response = jsn.loads(ret)
    response_details = json_response["details"]
    response_data = json_response["data"]

    response_data["records"] = sorted(
        response_data["records"],
        key=lambda x: datetime.strptime(x["creation_time"], "%Y-%m-%dT%H:%M:%S.%f"),
        reverse=time,
    )
    response_data = response_data["records"]

    if json:
        click.echo(json_response)
    else:
        prints.print_action(response_data, response_details, time)


def main():
    nlds_admin(prog_name="nlds-admin")


if __name__ == "__main__":
    nlds_admin()
