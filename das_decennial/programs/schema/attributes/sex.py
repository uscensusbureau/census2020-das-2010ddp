from programs.schema.attributes.abstractattribute import AbstractAttribute
from constants import CC

class SexAttr(AbstractAttribute):

    @staticmethod
    def getName():
        return CC.ATTR_SEX

    @staticmethod
    def getLevels():
        return {
                'Male'  : [0],
                'Female': [1]
            }