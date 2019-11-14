from programs.schema.attributes.abstractattribute import AbstractAttribute
from constants import CC


class HHMultiAttr(AbstractAttribute):

    @staticmethod
    def getName():
        return CC.ATTR_HHMULTI

    @staticmethod
    def getLevels():
        return {
                'Household does not have three or more generations' : [0],
                'Household has three or more generations': [1]
            }


    @staticmethod
    def recodeMultiTrue():
        name = CC.HHMULTI_TRUE
        groupings = {
                "Yes, multigen": [1]
            }
        return name, groupings

    @staticmethod
    def recodeMultiFalse():
        name = CC.HHMULTI_FALSE
        groupings = {
                "Not multigen": [0]
            }
        return name, groupings
