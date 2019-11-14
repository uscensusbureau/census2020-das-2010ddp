from programs.schema.attributes.abstractattribute import AbstractAttribute
from constants import CC


class HHRaceAttr(AbstractAttribute):

    @staticmethod
    def getName():
        return CC.ATTR_HHRACE

    @staticmethod
    def getLevels():
        return {
                'white'      : [0],
                'black'      : [1],
                'asian'      : [2],
                'aian'       : [3],
                'nhopi'       : [4],
                'sor'        : [5],
                'two or more': [6]
            }

    @staticmethod
    def recodeWhiteAlone():
        name = CC.HHRACE_WHITEALONE
        groupings = {
                "White alone": [0]
            }
        return name, groupings
