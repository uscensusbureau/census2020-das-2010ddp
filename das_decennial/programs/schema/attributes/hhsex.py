from programs.schema.attributes.abstractattribute import AbstractAttribute
from constants import CC

class HHSexAttr(AbstractAttribute):

    @staticmethod
    def getName():
        return CC.ATTR_HHSEX

    @staticmethod
    def getLevels():
        return {
                'Male'  : [0],
                'Female': [1]
            }

    @staticmethod
    def recodeMaleOnly():
        name = CC.HHSEX_MALE
        groupings = {
            "male only" : [0]
            }
        return name, groupings

    @staticmethod
    def recodeFemaleOnly():
        name = CC.HHSEX_FEMALE
        groupings = {
            "female only": [1]
            }
        return name, groupings
