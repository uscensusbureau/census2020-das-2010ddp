from programs.schema.attributes.abstractattribute import AbstractAttribute
from constants import CC


class HHAgeAttr(AbstractAttribute):

    @staticmethod
    def getName():
        return CC.ATTR_HHAGE

    @staticmethod
    def getLevels():
        return  {
                    'Householder 15 to 24 years'   : [0],
                    'Householder 25 to 34 years'   : [1],
                    'Householder 35 to 44 years'   : [2],
                    'Householder 45 to 54 years'   : [3],
                    'Householder 55 to 59 years'   : [4],
                    'Householder 60 to 64 years'   : [5],
                    'Householder 65 to 74 years'   : [6],
                    'Householder 75 to 84 years'   : [7],
                    'Householder 85 years and over': [8]
                }

    @staticmethod
    def recode15to24():
        name = CC.HHAGE_15TO24
        groupings = {
                    "Householder age is 15 to 24": [0]
            }
        return name, groupings

    @staticmethod
    def recode60plus():
        name = CC.HHAGE_60PLUS
        groupings = {
                "Householder age is 60 or more": [5, 6, 7, 8]
            }
        return name, groupings

    @staticmethod
    def recodeUnder60():
        name = CC.HHAGE_UNDER60
        groupings = {
                    "Householder age is under 60" : [0, 1, 2, 3, 4]
            }
        return name, groupings

    @staticmethod
    def recode60to64():
        name = CC.HHAGE_60TO64
        groupings = {
                    "Householder age is 60 to 64" : [5]
            }
        return name, groupings

    @staticmethod
    def recode65to74():
        name = CC.HHAGE_65TO74
        groupings = {
                    "Householder age is 65 to 74" : [6]
            }
        return name, groupings

    @staticmethod
    def recode75plus():
        name = CC.HHAGE_75PLUS
        groupings = {
                    "Householder age is 75 or older" : [7, 8]
            }
        return name, groupings
 
    @staticmethod
    def recodeHHover65():
        name = CC.HHAGE_OVER65
        groupings = {
                    "15 to 64 years"   : list(range(6)),
                    "65 years and over": list(range(6,9))
            }
        return name, groupings

    @staticmethod
    def recodeH18():
        name = CC.HHAGE_H18
        groupings = {
                "Householder 15 to 34 years"   : [0,1],
                "Householder 35 to 64 years"   : [2,3,4,5],
                "Householder 65 years and over": [6,7,8]
            }
        return name, groupings
