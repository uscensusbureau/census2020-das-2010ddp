from programs.schema.attributes.abstractattribute import AbstractAttribute
from constants import CC


class HHHispAttr(AbstractAttribute):

    @staticmethod
    def getName():
        return CC.ATTR_HHHISP

    @staticmethod
    def getLevels():
        return {
                'Not Hispanic or Latino': [0],
                'Hispanic or Latino'    : [1]
            }
