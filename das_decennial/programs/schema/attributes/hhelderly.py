from programs.schema.attributes.abstractattribute import AbstractAttribute
from constants import CC

class HHElderlyAttr(AbstractAttribute):

    @staticmethod
    def getName():
        return CC.ATTR_HHELDERLY

    @staticmethod
    def getLevels():
        return {
                   'None'                                        : [0],
                   'At least one 60+ year old person, no one 65+': [1],
                   'At least one 65+ year old person, no one 75+': [2],
                   'At least one 75+ year old person'            : [3]
               }


    @staticmethod
    def recodePresenceOver60():
        name = CC.HHELDERLY_PRESENCE_OVER60
        groupings = {
                        "At least one preson over 60 present" : [1, 2, 3]
            }
        return name, groupings

    @staticmethod
    def recodePresenceNot60to64():
        name = CC.HHELDERLY_PRESENCE_NOT_60TO64
        groupings = {
                        "No one between 60 to 64 present" : [0, 2, 3]
            }
        return name, groupings

    @staticmethod
    def recodePresenceNot65to74():
        name = CC.HHELDERLY_PRESENCE_NOT_65TO74
        groupings = {
                "No on between 65 to 74 present" : [0, 1, 3]
            }
        return name, groupings

    @staticmethod
    def recodePresenceNot75Plus():
        name = CC.HHELDERLY_PRESENCE_NOT_75PLUS
        groupings = {
                "No one 75+ present" : [0, 1, 2]
            }
        return name, groupings

    @staticmethod
    def recodePresenceNot65Plus():
        name = CC.HHELDERLY_PRESENCE_NOT_65PLUS
        groupings = {
                "No one 65+ present": [0, 1]
            }
        return name, groupings

    @staticmethod
    def recodePresenceNot60Plus():
        name = CC.HHAGE_PRESENCE_NOT_60PLUS
        groupings = {
                "No one 60 or older": [0]
            }
        return name, groupings

    @staticmethod
    def recodePresence65to74():
        name = CC.HHELDERLY_PRESENCE_65TO74
        groupings = {
                "No one 60 to 64 or 75 plus, at least one person 65 to 74" : [3]
            }
        return name, groupings


    @staticmethod
    def recodePresence60():
        name = CC.HHELDERLY_PRESENCE_60
        groupings = {
                "No one over 60": [0],
                "At least one over 60": [1,2,3]
            }
        return name, groupings

    @staticmethod
    def recodePresence65():
        name = CC.HHELDERLY_PRESENCE_65
        groupings = {
                "No one over 65 years": [0, 1],
                "At least one person over 65 years": [2, 3]
            }
        return name, groupings

    @staticmethod
    def recodePresence75():
        name = CC.HHELDERLY_PRESENCE_75
        groupings = {
                "No one over 75 years": [0, 1, 2],
                "At least one person over 75 years": [3]
            }
        return name, groupings
