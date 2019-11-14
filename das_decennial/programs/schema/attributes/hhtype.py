from programs.schema.attributes.abstractattribute import AbstractAttribute
from constants import CC


class HHTypeAttr(AbstractAttribute):

    @staticmethod
    def getName():
        return CC.ATTR_HHGQ

    @staticmethod
    def getLevels():
        return {
                'Married opposite-sex with own children under 18, under 6 yrs only'     : [0],
                'Married opposite-sex with own children under 18, between 6 and 17 only': [1],
                'Married opposite-sex with own children under 18, both ranges'          : [2],
                'Married opposite-sex no own children under 18'                         : [3],
                'Married same-sex with own children only under 6 yrs'                   : [4],
                'Married same-sex with own children between 6 and 17'                   : [5],
                'Married same-sex with own children in both ranges'                     : [6],
                'Married same-sex no own children under 18'                             : [7],
                'Cohabiting opposite-sex with own children only under 6 yrs'            : [8],
                'Cohabiting opposite-sex with own children between 6 and 17'            : [9],
                'Cohabiting opposite-sex with own children in both ranges'              : [10],
                'Cohabiting opposite-sex with relatives, no own children under 18'      : [11],
                'Cohabiting opposite-sex without relatives, no own children under 18'   : [12],
                'Cohabiting same-sex with own children only under 6 yrs'                : [13],
                'Cohabiting same-sex with own children between 6 and 17'                : [14],
                'Cohabiting same-sex with own children in both ranges'                  : [15],
                'Cohabiting same-sex with relatives, no own children under 18'          : [16],
                'Cohabiting same-sex without relatives, no own children under 18'       : [17],
                'No spouse or partner, alone'                                           : [18],
                'No spouse or partner with own children under 6'                        : [19],
                'No spouse or partner with own children between 6 and 17.'              : [20],
                'No spouse or partner with own children in both ranges'                 : [21],
                'No spouse or partner living with relatives but no own children'        : [22],
                'No spouse or partner and no relatives, not alone.'                     : [23]
            }

    @staticmethod
    def recodeHHtypeOwnChildrenUnderSix():
        name = CC.HHTYPE_OWNCHILD_UNDERSIX
        groupings = {
                "Householder has own child under 6" : [0, 2, 4, 6, 8, 10, 13, 15, 19, 21]
            }
        return name, groupings

    @staticmethod
    def recodeHHtypeOwnChildUnder18():
        name = CC.HHTYPE_OWNCHILD_UNDER18
        groupings = {
                "Householder has own child under 18" : [0, 1, 2, 4, 5, 6, 8, 9, 10, 13, 14, 15, 19, 20, 21]
            }
        return name, groupings

    @staticmethod
    def recodeFamily():
        name = CC.HHTYPE_FAMILY
        groupings = {
                "Family": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14, 15, 16, 19, 20, 21, 22]
            }
        return name, groupings

    @staticmethod
    def recodeNonfamily():
        name = CC.HHTYPE_NONFAMILY
        groupings = {
                "Non-Family": [12, 17, 18, 23]
            }
        return name, groupings

    @staticmethod
    def recodeMarriedFamily():
        name = CC.HHTYPE_FAMILY_MARRIED
        groupings = {
                "Married Family": list(range(0,8))
            }
        return name, groupings

    @staticmethod
    def recodeOtherFamily():
        name = CC.HHTYPE_FAMILY_OTHER
        groupings = {
                "Other Family": [8, 9, 10, 11, 13, 14, 15, 16, 19, 20, 21, 22]
            }
        return name, groupings

    @staticmethod
    def recodeHHtypeAlone():
        name = CC.HHTYPE_ALONE
        groupings = {
                "Alone": [18]
            }
        return name, groupings

    @staticmethod
    def recodeHHtypeNotAlone():
        """ everything but living alone """
        name = CC.HHTYPE_NOT_ALONE
        groupings = {
                "Not alone": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 19, 20, 21, 22, 23]
            }
        return name, groupings

    @staticmethod
    def recodeNonfamilyNotAlone():
        """ Nonfamily, not alone """
        name = CC.HHTYPE_NONFAMILY_NOT_ALONE
        groupings = {
                "Nonfamily, Not alone": [12, 17, 23]
            }
        return name, groupings

    @staticmethod
    def recodeHHtypeNotSize2():
        name = CC.HHTYPE_NOT_SIZE_TWO
        groupings = {
                "Types inconsistent with size 2": [0, 1, 2, 4, 5, 6, 8, 9, 10, 11, 13, 14, 15, 16, 18, 21]
            }
        return name, groupings

    @staticmethod
    def recodeHHtypeSizeTwoWithChild():
        name = CC.HHTYPE_SIZE_TWO_WITH_CHILD
        groupings = {
                "Size 2, householder with child" : [19, 20]
            }
        return name, groupings

    @staticmethod
    def recodeHHtypeSizeTwoCouple():
        name = CC.HHTYPE_SIZE_TWO_COUPLE
        groupings = {
                "Size 2, householder with spouse/unmarried partner" : [3, 7, 12, 17]
            }
        return name, groupings

    @staticmethod
    def recodeHHtypeNotSizeThree():
        name = CC.HHTYPE_NOT_SIZE_THREE
        groupings = {
                "Type not consistent with size 3": [2, 6, 10, 15, 18]
            }
        return name, groupings

    @staticmethod
    def recodeHHtypeSizeThreeNotMulti():
        name = CC.HHTYPE_SIZE_THREE_NOT_MULTI
        groupings = {
                "Size 3, can't be multi": [0, 1, 3, 4, 5, 7, 8, 9, 11, 12, 13, 14, 16, 17, 21, 23]
            }
        return name, groupings

    @staticmethod
    def recodeHHtypeSizeThreeWithTwoChildren():
        name = CC.HHTYPE_SIZE_THREE_WITH_TWO_CHILDREN
        groupings = {
                "Size 3, two children": [21]
            }
        return name, groupings

    @staticmethod
    def recodeHHtypeSizeThreeCoupleWithOneChild():
        name = CC.HHTYPE_SIZE_THREE_COUPLE_WITH_ONE_CHILD
        groupings = {
                "Size 3, couple with one child": [0, 1, 4, 5, 8, 9, 13, 14]
            }
        return name, groupings

    @staticmethod
    def recodeHHtypeSizeFourNotMulti():
        name = CC.HHTYPE_SIZE_FOUR_NOT_MULTI
        groupings = {
                "Size 4, can't be multigen": [2, 6, 10, 12, 15, 17, 23]
            }
        return name, groupings

    @staticmethod
    def recodeHHtypeSizeFourCoupleWithTwoChildren():
        name = CC.HHTYPE_SIZE_FOUR_COUPLE_WITH_TWO_CHILDREN
        groupings = {
                 "Size 4, couple with two children": [2, 6, 10, 15]
             }
        return name, groupings

    @staticmethod
    def recodeHHtypeNotMulti():
        name = CC.HHTYPE_NOT_MULTI
        groupings = {
                "Can never be multigen regardless of size" : [12, 17, 18, 23]
            }
        return name, groupings

    @staticmethod
    def recodeFamilyMarriedWithChildrenIndicator():
        name = CC.HHTYPE_FAMILY_MARRIED_WITH_CHILDREN_INDICATOR
        groupings = {
                "Married without own children under 18": [3, 7],
                "Married with own children under 18"   : [0, 1, 2, 4, 5, 6]
            }
        return name, groupings

    @staticmethod
    def recodeFamilyOtherWithChildrenIndicator():
        name = CC.HHTYPE_FAMILY_OTHER_WITH_CHILDREN_INDICATOR
        groupings = {
                "Other family without own children under 18": [11, 16],
                "Other family with own children under 18"   : [8, 9, 10, 13, 14, 15]
            }
        return name, groupings

    @staticmethod
    def recodeHHtypeCohabiting():
        name = CC.HHTYPE_COHABITING
        groupings = {
            HHTYPE: {
                "Cohabiting": list(range(8, 18))
            }
        }
        return name, groupings

    @staticmethod
    def recodeHHtypeCohabitingWithChildrenIndicator():
        name = CC.HHTYPE_COHABITING_WITH_CHILDREN_INDICATOR
        groupings = {
                "Cohabiting without own children under 18" : [11, 12, 16, 17],
                "Cohabiting with own children under 18"    : [8, 9, 10, 13, 14, 15]
            }
        return name, groupings

    @staticmethod
    def recodeHHtypeNoSpouseOrPartner():
        name = CC.HHTYPE_NO_SPOUSE_OR_PARTNER
        groupings = {
                "No spouse or partner": list(range(18, 24))
            }
        return name, groupings

    @staticmethod
    def recodeHHtypeNoSpouseOrPartnerLevels():
        name = CC.HHTYPE_NO_SPOUSE_OR_PARTNER_LEVELS
        groupings = {
                "Alone": [18],
                "With own children": [19, 20, 21],
                "With relatives, no own children": [22],
                "No relatives, not alone": [23]
            }
        return name, groupings

    @staticmethod
    def recodeFamilyMarriedWithChildrenLevels():
        name = CC.HHTYPE_FAMILY_MARRIED_WITH_CHILDREN_LEVELS
        groupings = {
                "Married with children under 6 only"                   : [0,4],
                "Married with children 6 to 17 years only"             : [1,5],
                "Married with children under 6 years and 6 to 17 years": [2,6]
            }
        return name, groupings

    @staticmethod
    def recodeFamilyOtherWithChildrenLevels():
        name = CC.HHTYPE_FAMILY_OTHER_WITH_CHILDREN_LEVELS
        groupings = {
                "Other family with children under 6 only"                   : [8,13,19],
                "Other family with children 6 to 17 years only"             : [9,14,20],
                "Other family with children under 6 years and 6 to 17 years": [10,15,21]
            }
        return name, groupings

    @staticmethod
    def recodeHHtypeCoupleLevels():
        name = CC.HHTYPE_COUPLE_LEVELS
        groupings = {
                "Married": list(range(8)),
                "Unmarried Partner": list(range(8,18)),
                "All others": list(range(18,24))
            }
        return name, groupings

    @staticmethod
    def recodeHHtypeOppositeSexLevels():
        name = CC.HHTYPE_OPPOSITE_SEX_LEVELS
        groupings = {
                "Married Opposite": list(range(4)),
                "Partner Opposite": list(range(8,13))
            }
        return name, groupings

    @staticmethod
    def recodeHHtypeSameSexLevels():
        name = CC.HHTYPE_SAME_SEX_LEVELS
        groupings = {
                "Married Same": list(range(4,8)),
                "Partner Same": list(range(13,18))
            }
        return name, groupings
