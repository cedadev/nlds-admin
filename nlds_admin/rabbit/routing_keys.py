# encoding: utf-8
"""
routing_keys.py
"""

__author__ = "Neil Massey and Jack Leland"
__date__ = "24 Feb 2025"
__copyright__ = "Copyright 2025 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

# Refactored routing keys into their own file

# Routing key constants
LIST = "list"
STAT = "stat"
FIND = "find"
META = "meta"
CANCEL = "cancel"
CATALOG_Q = "catalog_q_user"
MONITOR_Q = "monitor_q_user"
NLDS_Q = "nlds_q"
START = "start"
# Exchange routing key parts – root
ADMIN = "nlds-admin"

# Monitor keys for sending complete monitor records
ROOT = "nlds-api"
MONITOR_PUT = "monitor-put"
