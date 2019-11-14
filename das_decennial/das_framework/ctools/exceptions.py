""" Module for implementing exceptions thrown by disclosure avoidance systems"""
# 4/9/2019
# Pavel Zhuravlev

import logging

class Error(Exception):
    """Base class for DAS exceptions."""

    def __init__(self, msg=''):
        self.message = msg
        # Log it. One of the main reasons to have this class
        logging.error(msg)
        Exception.__init__(self, msg)

    def __repr__(self):
        return self.message

    __str__ = __repr__


class DASConfigError(Error):
    """ Errors resulting from parsing the config file"""

    def __init__(self, msg, option, section):
        if option is not None:
            message = f"{msg}: No option '{option}' in config section: [{section}]"
        else:
            message = f"{msg}: No section '{section}' in config file"
        Error.__init__(self, message)
        self.option = option
        self.section = section
        self.args = (option, section)


class IncompatibleAddendsError(Error):
    """ Errors when two objects of different type or with unaddable attributes are added"""

    def __init__(self, msg, attrname, addend1attr, addend2attr):
        Error.__init__(self, f"{msg} cannot be added: addends have different {attrname}: {addend1attr} != {addend2attr}")
        self.attrname = attrname
        self.addend1attr = addend1attr
        self.addend2attr = addend2attr
        self.args = (attrname, addend1attr, addend2attr)

class DASValueError(Error):
    def __init__(self, msg, value):
        Error.__init__(self, f"{msg}: value '{value}' is invalid")
        self.value = value
        self.args = (value, )