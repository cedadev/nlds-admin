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
CATALOG_Q = "catalog_q"

# Exchange routing key parts – root
ADMIN = "nlds-admin"