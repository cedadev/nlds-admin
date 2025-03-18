.. _command-ref:

Command Line Reference
======================

The primary method of interacting with the Near-Line Data Store is through the
command line, which can be installed using the instructions. These admin tools are 
able to view jobs ran by any user from any group.

Users must specify a command to the ``nlds-admin`` and options and arguments for that 
command.

``nlds-admin [OPTIONS] COMMAND [ARGS]...``

As an overview the commands are:

Commands:
  | ``list     List holdings.``
  | ``find     Find and list files.``
  | ``stat     List transactions.``

Each command has its own specific options.  The argument is generally the file
or filelist that the user wishes to operate on.  The full command listing is
given below.

.. click:: nlds_admin.nlds_admin:nlds_admin
   :prog: nlds-admin
   :nested: full