""" Module for implementing specific das_decennial exceptions (more generic disclosure avoidance exceptions are in ctools/exceptions.py"""
# 5/14/2019
# Pavel Zhuravlev

import logging

class NodeRDDValidationError(Exception):
    """ Error for when some elements (nodes) of RDD fail indicated criteria"""

    def __init__(self, msg, sample_msg, sample):
        Exception.__init__(self, f"{msg}\n{sample_msg} {sample}")
        logging.error(msg)
        self.msg = msg
        self.sample_msg = sample_msg
        self.sample = sample