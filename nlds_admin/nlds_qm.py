import click
from nlds_admin.rabbit.publisher import RabbitMQPublisher
from nlds_admin.rabbit.consumer import RabbitMQConsumer
from nlds_admin.rabbit import message_keys as MSG
import json
import uuid
import base64
import zlib
import os
import os.path

"""
Utility program to manipulate messages in the NLDS queue.
The following operations can be performed:
1. split: if they have too many files to complete before the message timeout.
"""


def compress_data(data_dict):
    """Compress the data part of the message.  Only compatible with v1.0.11 of NLDS"""
    byte_string = json.dumps(data_dict).encode("ascii")
    data_dict_out = base64.b64encode(zlib.compress(byte_string, level=1)).decode(
        "ascii"
    )
    return data_dict_out


def decompress_data(data_dict):
    byte_string = data_dict.encode("ascii")
    decompressed_string = zlib.decompress(base64.b64decode(byte_string))
    dicto = json.loads(decompressed_string)
    return dicto


@click.group()
def nlds_qm():
    pass


@nlds_qm.command("split", help="Split messages.")
@click.option(
    "-q", "--queue", default="", type=str, help="Queue name to split messages for."
)
@click.option(
    "-n",
    "--number",
    default=1,
    type=int,
    help="Number of messages in queue to retrieve then split.",
)
@click.option(
    "-l", "--length", default=1000, type=int, help="Number of files per message."
)
@click.option(
    "-c",
    "--compress",
    default=False,
    type=bool,
    is_flag=True,
    help="Compress the DATA part of the message",
)
def split(queue, number, length, compress=False):
    """Split the messages in the queue so that they have <length> files in them as a
    maximum"""
    # consume a message off the queues and interpret as JSON
    consumer = RabbitMQConsumer(queue)
    for n in range(0, number):
        method, properties, body = consumer.consume_one_message()
        # get the routing key from the method
        rk = method.routing_key
        body_json = json.loads(body)
        # the message consists of the DETAILS and the DATA part
        details = body_json[MSG.DETAILS]
        # if the message is compressed then decompress it
        if MSG.COMPRESS in details and details[MSG.COMPRESS]:
            data = decompress_data[MSG.DATA]
        else:
            data = body_json[MSG.DATA]
        # get a list of files and split it
        files = data[MSG.FILELIST]
        file_sublist = [files[i : i + length] for i in range(0, len(files), length)]
        fn = 0
        click.echo(
            f"Working on message: {details[MSG.TRANSACT_ID]}, "
            f"user: {details[MSG.USER]}, "
            f"group: {details[MSG.GROUP]}, "
        )
        for f in file_sublist:
            # new sub id if new files
            if fn > 0:
                sub_id = uuid.uuid4()
            else:
                sub_id = details[MSG.SUB_ID]
            fn += 1
            click.echo(
                f"    Creating new message and changing sub id from "
                f"{details[MSG.SUB_ID]} to {sub_id}"
            )
            # reform the dictionary
            details[MSG.SUB_ID] = str(sub_id)
            data[MSG.FILELIST] = f
            if compress:
                comp_data = compress_data(data)
                details[MSG.COMPRESS] = True
                new_msg_dict = {MSG.DETAILS: details, MSG.DATA: comp_data}
            else:
                new_msg_dict = {MSG.DETAILS: details, MSG.DATA: data}
            consumer.publish_message(rk, new_msg_dict, properties=properties)
        click.echo("Number of sub messages", len(file_sublist))
        consumer.basic_ack(method=method)
    consumer.close()


def print_details(details, data, rk):
    if MSG.COMPRESS in details and details[MSG.COMPRESS]:
        data_uc = decompress_data(data)
    else:
        data_uc = data
    n_files = len(data_uc[MSG.FILELIST])
    click.echo(
        f"{details[MSG.USER]:<16}"
        f"{details[MSG.GROUP]:<12}"
        f"{details[MSG.TRANSACT_ID]:<38}"
        f"{details[MSG.SUB_ID]:<38}"
        f"{rk:<32}"
        f"{n_files:>10}"
    )


@nlds_qm.command("list", help="list messages.")
@click.option(
    "-q", "--queue", default="", type=str, help="Queue name to list messages for."
)
@click.option(
    "-n",
    "--number",
    default=1,
    type=int,
    help="Number of messages in queue to retrieve then split.",
)
def list(queue, number):
    # consume a message off the queues and interpret as JSON
    consumer = RabbitMQConsumer(queue)
    click.echo(
        f"{'user':<16}{'group':<12}{'transaction_id':<38}{'sub_id':<38}{'rk':<32}{'N files':>10}"
    )
    for n in range(0, number):
        method, properties, body = consumer.consume_one_message()
        # get the routing key from the method
        rk = method.routing_key
        body_json = json.loads(body)
        # the message consists of the DETAILS and the DATA part
        details = body_json[MSG.DETAILS]
        data = body_json[MSG.DATA]
        print_details(details, data, rk)
        consumer.channel.basic_nack(method.delivery_tag)


@nlds_qm.command("dump", help="dump messages.")
@click.option(
    "-q", "--queue", default="", type=str, help="Queue name to dump messages for."
)
@click.option(
    "-n",
    "--number",
    default=1,
    type=int,
    help="Number of messages in queue to retrieve then split.",
)
@click.option(
    "-r", "--target", default="/", type=str, help="Target directory to write to"
)
@click.option(
    "-l", "--length", default=1000, type=int, help="Number of files per message."
)
@click.option(
    "-c",
    "--compress",
    default=False,
    type=bool,
    is_flag=True,
    help="Compress the DATA part of the message",
)
def dump(queue, number, target, length, compress=False):
    """Dump the messages as JSON files."""
    # check the target directory exists
    if not os.path.exists(target):
        os.mkdir(target)
    # check the queue directory exists
    queue_dir = os.path.join(target, queue)
    if not os.path.exists(queue_dir):
        os.mkdir(queue_dir)
    consumer = RabbitMQConsumer(queue)

    for n in range(0, number):
        method, properties, body = consumer.consume_one_message()
        body_json = json.loads(body)
        # Get the routing key
        rk = method.routing_key
        # the message consists of the DETAILS and the DATA part
        details = body_json[MSG.DETAILS]
        # get the transaction_id
        transaction_id = details[MSG.TRANSACT_ID]
        # create a directory for the transaction
        trans_dir = os.path.join(queue_dir, transaction_id)
        if not os.path.exists(trans_dir):
            os.mkdir(trans_dir)

        # if the message is compressed then decompress it
        if MSG.COMPRESS in details and details[MSG.COMPRESS]:
            data = decompress_data(body_json[MSG.DATA])
        else:
            data = body_json[MSG.DATA]
        # get a list of files and split it
        files = data[MSG.FILELIST]
        file_sublist = [files[i : i + length] for i in range(0, len(files), length)]
        fn = 0
        click.echo(
            f"Working on message: {details[MSG.TRANSACT_ID]}, "
            f"user: {details[MSG.USER]}, "
            f"group: {details[MSG.GROUP]}, "
        )
        for f in file_sublist:
            # new sub id if new files
            if fn > 0:
                sub_id = uuid.uuid4()
            else:
                sub_id = details[MSG.SUB_ID]
            fn += 1
            click.echo(
                f"    Saving new message and changing sub id from "
                f"{details[MSG.SUB_ID]} to {sub_id}"
            )
            # reform the dictionary
            details[MSG.SUB_ID] = str(sub_id)
            details["routing_key"] = rk
            data[MSG.FILELIST] = f
            if compress:
                comp_data = compress_data(data)
                details[MSG.COMPRESS] = True
                new_msg_dict = {MSG.DETAILS: details, MSG.DATA: comp_data}
            else:
                new_msg_dict = {MSG.DETAILS: details, MSG.DATA: data}

            # the file name is the subid
            fname = os.path.join(trans_dir, details[MSG.SUB_ID])
            # open the file
            with open(fname, "bw") as fh:
                msg = json.dumps(new_msg_dict)
                fh.write(msg.encode("ascii"))

        consumer.channel.basic_ack(method.delivery_tag)


@nlds_qm.command("load", help="load messages.")
@click.option(
    "-q", "--queue", default="", type=str, help="Queue name to load messages for."
)
@click.option(
    "-t",
    "--transact_id",
    default="",
    type=str,
    help="Transaction id name to load messages for.",
)
@click.option(
    "-r", "--target", default="/", type=str, help="Target directory to read to"
)
@click.option(
    "-c",
    "--compress",
    default=False,
    type=bool,
    is_flag=True,
    help="Compress the DATA part of the message",
)
def load(queue, transact_id, target, compress=False):
    """Test that the dumped file can be read"""
    queue_dir = os.path.join(target, queue)
    trans_dir = os.path.join(queue_dir, transact_id)
    sub_files = os.listdir(trans_dir)
    for s in sub_files:
        sub_name = os.path.join(trans_dir, s)
        with open(sub_name, "br") as fh:
            msg = fh.read()
        msg_body = json.loads(msg)
        details = msg_body[MSG.DETAILS]
        # is the data compressed?
        if MSG.COMPRESS in details and details[MSG.COMPRESS]:
            data = decompress_data(msg_body[MSG.DATA])
        else:
            data = msg[MSG.DATA]
        click.echo(details, data)


def main():
    nlds_qm(prog_name="nlds-admin")


if __name__ == "__main__":
    nlds_qm()
