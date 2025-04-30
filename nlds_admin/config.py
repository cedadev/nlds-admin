# encoding: utf-8
"""
server_config.py
"""
__author__ = "Neil Massey and Jack Leland"
__date__ = "30 Nov 2021"
__copyright__ = "Copyright 2024 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

import json
import os.path

CONFIG_FILE_LOCATION = "~/.nlds-admin-config"
# Config file section strings
AUTH_CONFIG_SECTION = "authentication"

RABBIT_CONFIG_SECTION = "rabbitMQ"
RABBIT_CONFIG_EXCHANGES = "exchanges"
RABBIT_CONFIG_QUEUES = "queues"
RABBIT_CONFIG_QUEUE_NAME = "name"
RABBIT_CONFIG_EXCHANGE_DELAYED = "delayed"
RABBIT_CONFIG_PORT = "port"
RABBIT_CONFIG_TIMEOUT = "timeout"
RABBIT_CONFIG_HEARTBEAT = "heartbeat"

# Defines the compulsory server config file sections
CONFIG_SCHEMA = (
    (
        RABBIT_CONFIG_SECTION,
        ("user", "password", "server", "admin_port", "vhost", "exchange",),
    ),
)


def validate_config_file(json_config: dict) -> None:
    """
    Validate the JSON config file matches the schema defined in nlds_setup.
    Currently only checks that required headings and subheadings exist, i.e.
    only scans one layer deep and does no value checking.

    :param json_config:     Config file loaded using json.load()

    """
    # Convert defined schema into a dictionary for ease of iteration
    schema = dict(CONFIG_SCHEMA)

    # Loop through and check that required headings and labels exist
    for section_heading, section_labels in schema.items():
        try:
            section = json_config[section_heading]
        except KeyError:
            raise RuntimeError(
                f"The config file at {CONFIG_FILE_LOCATION} does not contain "
                f"a(n) ['{section_heading}'] section."
            )
        for sl in section_labels:
            if sl not in section:
                raise KeyError(
                    f"The config file at {CONFIG_FILE_LOCATION} does not "
                    f"contain '{sl}' in the ['{section}'] section."
                )


def load_config(config_file_path: str = CONFIG_FILE_LOCATION) -> dict:
    """
    Config file for the server contains authentication and rabbitMQ sections,
    the required contents of which are set by the schema in utils.constants.
    This function opens the config file (at a preset, configurable location)
    then verifies it.

    :parameter config_file_path:
    :type str:

    """
    # Location of config file is ./.server_config.  Open it, checking that it
    # exists as well.
    try:
        fh = open(os.path.expanduser(f"{config_file_path}"))
    except FileNotFoundError:
        raise FileNotFoundError(
            f"{config_file_path}", "The config file cannot be found."
        )

    # Load the JSON file, ensuring it is correctly formatted
    try:
        json_config = json.load(fh)
    except json.JSONDecodeError as je:
        raise RuntimeError(
            f"The config file at {config_file_path} has an error at "
            f"character {je.pos}: {je.msg}."
        )

    # Check that the JSON file contains the correct keywords / is in the correct
    # format
    validate_config_file(json_config)

    return json_config
