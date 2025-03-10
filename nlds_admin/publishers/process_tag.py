"""
process_tag.py
"""
__author__ = "Neil Massey"
__date__ = "24 Feb 2025"
__copyright__ = "Copyright 2025 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "neil.massey@stfc.ac.uk"

def process_tag(tag):
    """Process a tag in string format into dictionary format"""
    tag_dict = {}
    # strip "{" "}" symbolsfirst
    tag_list = (tag.replace("{", "").replace("}", "")).split(",")
    for tag_i in tag_list:
        tag_kv = tag_i.split(":")
        if len(tag_kv) < 2:
            continue
        tag_dict[tag_kv[0]] = tag_kv[1]
    return tag_dict