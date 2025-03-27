# nlds-admin

Administration tools for Near-Line Data Store (NLDS).
Instructions are for the development version - subject to change.

## Installation

1. Create a virtual environment for Python3:

    `python3 -m venv ~/nlds-admin-venv`

2. Activate the environment:

    `source ~/nlds-admin-venv/bin/activate`

3. Install the requirements:

    `pip install ./`

4. Run the command from the source directory:

    `nlds-admin <command> <options>`

## Config file

`nlds-admin` requires a config file, called `.config` in the same source directory as the `nlds-admin.py` file.  There is a template for this file in the source directory called `config-template`.  To create a valid `.config` file, carry out these steps:

1. Copy the `config-template` to `.config`

    `cp config-template .config`

2. Edit `.config` and fill in `rabbit_server`, `rabbit_user`, `rabbit_vhost`, `rabbit_password` in the following sections:

    ```
    "server": {{ rabbit_server }},
    "user":"{{ rabbit_user }}",
    "vhost":"{{ rabbit_vhost }}",
    "password":"{{ rabbit_password }}"
    ```

3. Only JASMIN administrators have this information, as this tool is designed to be used only by JASMIN admins.  Ask Neil, Danny or Chami for this information.