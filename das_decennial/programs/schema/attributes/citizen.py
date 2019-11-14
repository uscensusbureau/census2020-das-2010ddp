from programs.schema.attributes.abstractattribute import AbstractAttribute
from constants import CC


class CitizenAttr(AbstractAttribute):

    @staticmethod
    def getName():
        return CC.ATTR_CITIZEN

    @staticmethod
    def getLevels():
        return {
                    'Non-citizen': [0],
                    'Citizen'    : [1]
                }