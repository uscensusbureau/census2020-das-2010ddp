import programs.schema.schema as sk
from programs.schema.schemas.schema_factory import SchemaFactory


# Stub for backward compatibility
def buildSchema(path=None):
    return SchemaHousehold2010Factory(name="Household2010",
                                      dimnames=[HHSEX, HHAGE, HISP, RACE, SIZE, HHTYPE, ELDERLY, MULTI],
                                      shape=(2, 9, 2, 7, 8, 24, 4, 2), path=path).getSchema()


class SchemaHousehold2010Factory(SchemaFactory):

    HHSEX = "hhsex"
    HHAGE = "hhage"
    HISP = "hisp"
    RACE = "hhrace"
    SIZE = "size"
    HHTYPE = "hhtype"
    ELDERLY = "elderly"
    MULTI = "multi"

    def addRecodes(self):
        for recode in self.getRecodesToAdd():
            self.schema.addRecode(*recode())


    def getRecodesToAdd(self):
        ###############################################################################
        # Recodes for Household2010
        ###############################################################################
        recodes_to_perform = (

            self.whiteonly,
            self.isWhiteAlone,
            self.isBlackAlone,
            self.isAIANAlone,
            self.isAsianAlone,
            self.isNHOPIAlone,
            self.isSORAlone,
            self.isTMRaces,

            self.isHisp,
            self.isNotHisp,
            self.family,
            self.nonfamily,
            self.marriedfamily,
            self.married_with_children_indicator,
            self.married_with_children_levels,
            self.married_p38_interior,

            self.otherfamily,
            self.other_with_children_indicator,
            self.other_with_children_levels,
            self.other_p38_interior,

            self.alone,
            self.invert_alone,
            self.notalone,
            self.non2type,
            self.type2_onechild,
            self.type2_couple,
            self.non3type,
            self.type3_twochild,
            self.type3_notmulti,
            self.type3_couple_with_child,

            self.type4_notmulti,
            self.type4_couple_with_twochildren,

            self.size1,
            self.size2,
            self.size3,
            self.size4,
            self.size2plus,
            self.sizex0,
            self.sizex01,
            self.vacant,
            self.isOccupied,

            self.cohabiting,
            self.cohabiting_with_children_indicator,

            self.no_spouse_or_partner,
            self.no_spouse_or_partner_levels,

            self.presence60,
            self.presence65,
            self.presence75,
            self.couplelevels,

            self.oppositelevels,
            self.samelevels,
            self.allother,
            self.hhover65,
            self.hhageH18,
            self.solo,

            self.age_15to24,
            self.age_under60,
            self.age_60to64,
            self.age_65to74,
            self.age_75plus,
            self.age_60plus,
            self.age_65plus,
            self.pres_under60,
            self.pres_under65,

            self.yes_multi,
            self.no_multi,

            self.never_multi,

            self.male,
            self.female,

            self.ownchild_under6,
            self.ownchild_under18,

            self.elderly3,

            self.invert_elderly0,
            self.invert_elderly1,
            self.invert_elderly2,
            self.invert_elderly3,
        )
        return recodes_to_perform


        ###############################################################################
        # Tables for SF1
        ###############################################################################
        # P1 - P14 not covered

        # P15 HISP, HISP * RACE

        # P16-17 not covered

        # P18, family, nonfamily, marriedfamily, otherfamily, otherfamily * HHSEX, alone, notalone
        # Family
        #    married fam
        #    other fam
        #        male, no spouse
        #        remale, no spouse
        # Nonfamily
        #    alone
        #    not alone

        # P19, size1, size2plus, HHSEX * size1, family, married, married_with_children_indicator, otherfamily, HHSEX * otherfamily  HHSEX * other_with_children_indicator, notalone, HHSEX * notalone
        # 1-person household
        #    male
        #    female
        # 2 or more person
        #    family
        #        married
        #            with own children under 18
        #            without own children under 18
        #        other fam
        #            male, no spouse
        #                with own children
        #                without own children
        #            female, no spouse
        #                with own children
        #                without own children
        #    nonfam
        #        male
        #        female

        # P20, marriedfamily, married_with_children_indicator, cohabiting, cohabiting_with_children_indicator, HHSEX * no_spouse_or_partner, HHSEX * no_spouse_or_partner_levels
        # married
        #    with own child
        #    without own child
        # cohabiting
        #    with own child
        #    wihout own child
        # male, no spouse or partner
        #    alone
        #    with own child
        #    with relative, without own child
        #    no relatives, not alone
        # female, no spouse or partner
        #    alone
        #    with own child
        #    with relative, without own child
        #    no relatives, not alone

        # P22 family, HHAGE * family, nonfamily, HHAGE * nonfamily

        # P23 presence60, family * presence60, marriedfamily * presence60, otherfamily * presence60, HHSEX * otherfamily * presence60, nonfamily * presence60
        # over 60
        #    Family
        #        married
        #        other
        #            male, no spouse
        #            female, no spouse
        #    nonfam
        # no over 60
        #    fam
        #        married
        #        other
        #            male, no spouse
        #            female, no spouse
        #    nonfam

        # P24 presence60, size1 * presence60, size2plus * presence60, notalone * presence60, family * presence60
        # over 60
        #    1 per
        #    2+ per
        #        fam
        #        nonfam
        # no over 60
        #    1 per
        #    2+ per
        #        fam
        #        nonfam

        # P25 presence65, size1 * presence65, size2plus * presence65, notalone * presence65, family * presence65

        # P26 presence75, size1 * presence75, size2plus * presence75, notalone * presence75, family * presence75

        # P28 family, sizex01 * family, nonfamily, sizex0 * nonfamily

        # P29-P37 not covered

        # P38 marriedfamily, married_with_children_indicator, married_with_children_levels, otherfamily, HHSEX * otherfamily, HHSEX * other_with_children_indicator, HHSEX * other_with_children_levels
        # married
        #    with children
        #        levels
        #    without
        # other
        #    male, no spouse
        #        with children
        #            levels
        #        without
        #    female, no spouse
        #        with children
        #            levels
        #        without

        # P40 -P43 not covered

        # P12A-P17I not covered

        # P18A-G RACE * family, RACE * nonfamily, RACE * marriedfamily, RACE * otherfamily, RACE * otherfamily * HHSEX, RACE * alone, RACE * notalone

        # P18H HISP * family, HISP * nonfamily, HISP * marriedfamily, HISP * otherfamily, HISP * otherfamily * HHSEX, HISP * alone, HISP * notalone

        # P18I whiteonly * HISP * family, whiteonly * HISP * nonfamily, whiteonly * HISP * marriedfamily, whiteonly * HISP * otherfamily, whiteonly * HISP * otherfamily * HHSEX, whiteonly * HISP * alone, whiteonly * HISP * notalone

        # P28A-G RACE * family, RACE * family * sizex01, RACE * nonfamily, RACE * nonfamily * sizex0

        # P28H HISP * family, HISP * family * sizex01, HISP * nonfamily, HISP * nonfamily * sizex0

        # P28I whiteonly * HISP * family, whiteonly * HISP * family * sizex01, whiteonly * HISP * nonfamily, whiteonly * HISP * nonfamily * sizex0

        # P29A-P37I not covered

        # P38A -G RACE * marriedfamily, RACE * married_with_children_indicator, RACE * married_with_children_levels, RACE * otherfamily, RACE * HHSEX * otherfamily, RACE * HHSEX * other_with_children_indicator, RACE * HHSEX * other_with_children_levels

        # P38H HISP * marriedfamily, HISP * married_with_children_indicator, HISP * married_with_children_levels, HISP * otherfamily, HISP * HHSEX * otherfamily, HISP * HHSEX * other_with_children_indicator, HISP * HHSEX * other_with_children_levels


        # P38I whiteonly * HISP * marriedfamily, whiteonly * HISP * married_with_children_indicator, whiteonly * HISP * married_with_children_levels, whiteonly * HISP * otherfamily,HHSEX * otherfamily, whiteonly * HISP * HHSEX * other_with_children_indicator, whiteonly * HISP * HHSEX * other_with_children_levels

        # PCO1 -PCO43I not covered

        # PCT-WHT1 - C-PCT11 not covered

        # PCT12 - PCT13 not covered

        # PCT14 MULTI

        # PCT15 couplelevels, oppositelevels, samelevels, HHSEX * samelevels

        # PCT18 HHSEX * nonfamily, HHSEX * alone, HHSEX * alone * hhover65, HHSEX * notalone, HHSEX * notalone * hhover65

        # PCT21 - PCT22 not covered

        # PCT13A - I not covered

        # PCT14A - I RACE * MULTI, HISP * MULTI, whiteonly * HISP * MULTI

        # H1 - H5 not covered.

        # H6 RACE

        # H7 HISP * RACE

        # H10 - H12 not covered

        # H13 sizex0

        # H14 RENT, RENT * RACE

        # H15 RENT, RENT * HISP

        # H16 RENT, RENT * sizex0

        # H17 RENT, RENT * HHAGE

        # H18 RENT, RENT * family, RENT * marriedfamily, RENT * marriedfamily * HHAGE, RENT * otherfamily, RENT * otherfamily * HHSEX, RENT * otherfamily * HHSEX * HHAGE, RENT * nonfamily, RENT * nonfamily * HHSEX, RENT * alone * HHSEX * HHAGE, RENT * notalone * HHSEX * HHAGE. (AGE technically needs a new recode here)

        # H11A - H12I not covered

        # H16A - I RACE * RENT, RACE * RENT * sizex0, HISP * RENT, HISP * RENT * sizex0, whiteonly * HISP * RENT, whiteonly * HISP * RENT * sizex0

        # H17A - I RACE * RENT, HISP * RENT, whiteonly * HISP * RENT, RACE * RENT * HHAGE, HISP * RENT * HHAGE, whiteonly * HISP * RENT * HHAGE

        # HCT1 RENT, RENT * HISP, RENT * HISP * RACE

        # HCT2 needs recodes. RENT * with_children_indicator, RENT * with_children_levels

        # HCT4 not covered.

    @staticmethod
    def getLevels():
        leveldict = {
            HHSEX: {
                'Male'  : [0],
                'Female': [1]
            },

            HHAGE: {
                'Householder 15 to 24 years'   : [0],
                'Householder 25 to 34 years'   : [1],
                'Householder 35 to 44 years'   : [2],
                'Householder 45 to 54 years'   : [3],
                'Householder 55 to 59 years'   : [4],
                'Householder 60 to 64 years'   : [5],
                'Householder 65 to 74 years'   : [6],
                'Householder 75 to 84 years'   : [7],
                'Householder 85 years and over': [8]
            },

            HISP: {
                'Not Hispanic or Latino': [0],
                'Hispanic or Latino'    : [1]
            },
            #The 2010 specs show 00s as not in universe. Do we not have to worry about those records?
            RACE: {
                'Householder is White alone'      : [0],
                'Householder is Black alone'      : [1],
                'Householder is AIAN alone'      : [2],
                'Householder is Asian alone'       : [3],
                'Householder is NHOPI alone'       : [4],
                'Householder is SOR alone'        : [5],
                'Householder is two or more races': [6]
            },

            SIZE: {
                '0-person household'        : [0],
                '1-person household'        : [1],
                '2-person household'        : [2],
                '3-person household'        : [3],
                '4-person household'        : [4],
                '5-person household'        : [5],
                '6-person household'        : [6],
                '7-or-more-person household': [7]
            },

            HHTYPE: {
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
            },

            #RENT: {
            #    'Owned' : [0],
            #    'Rented': [1]
            #},

            ELDERLY: {
                'None'                            : [0],
                'At least one 60+ year old person, no one 65+': [1],
                'At least one 65+ year old person, no one 75+': [2],
                'At least one 75+ year old person': [3]
            },

            MULTI: {
                'Household does not have three or more generations' : [0],
                'Household has three or more generations': [1]
            },

        }
        return leveldict


    ###############################################################################
    # Recode functions
    ###############################################################################
    @staticmethod
    def invert_elderly0():
        name = "invert_elderly0"
        groupings = {
            ELDERLY: {
                "Not none" : [1, 2, 3]
            }
        }
        return name, groupings

    @staticmethod
    def invert_elderly1():
        name = "invert_elderly1"
        groupings = {
            ELDERLY: {
                "Not at least one person 60+,but no one 65+" : [0, 2, 3]
            }
        }
        return name, groupings

    @staticmethod
    def invert_elderly2():
        name = "invert_elderly2"
        groupings = {
            ELDERLY: {
                "Not at least one person 65+, but no one 75+" : [0, 1, 3]
            }
        }
        return name, groupings

    @staticmethod
    def invert_elderly3():
        name = "invert_elderly3"
        groupings = {
            ELDERLY: {
                "Not at least one person 75+" : [0, 1, 2]
            }
        }
        return name, groupings

    @staticmethod
    def pres_under65():
        name = "pres_under65"
        groupings = {
            ELDERLY: {
                "No one 65 or older": [0, 1]
            }
        }
        return name, groupings

    @staticmethod
    def pres_under60():
        name = "pres_under60"
        groupings = {
            ELDERLY: {
                "No one 60 or older": [0]
            }
        }
        return name, groupings

    @staticmethod
    def elderly3():
        name = "elderly3"
        groupings = {
            ELDERLY: {
                "" : [3]
            }
        }
        return name, groupings

    @staticmethod
    def ownchild_under6():
        name = "ownchild_under6"
        groupings = {
            HHTYPE: {
                "Householder has own child under 6" : [0, 2, 4, 6, 8, 10, 13, 15, 19, 21]
            }
        }
        return name, groupings

    @staticmethod
    def ownchild_under18():
        name = "ownchild_under18"
        groupings = {
            HHTYPE: {
                "Householder has own child under 18" : [0, 1, 2, 4, 5, 6, 8, 9, 10, 13, 14, 15, 19, 20, 21]
            }
        }
        return name, groupings

    @staticmethod
    def male():
        name = "male"
        groupings = {
            HHSEX: {
                "male": [0]
            }
        }
        return name, groupings

    @staticmethod
    def female():
        name = "female"
        groupings = {
            HHSEX: {
                "female": [1]
            }
        }
        return name, groupings

    @staticmethod
    def age_15to24():
        name = "age_15to24"
        groupings = {
            HHAGE: {
                "Householder age is 15 to 24": [0]
            }
        }
        return name, groupings

    @staticmethod
    def age_60plus():
        name = "age_60plus"
        groupings = {
            HHAGE: {
                "Householder age is 60 or more": [5, 6, 7, 8]
            }
        }
        return name, groupings

    @staticmethod
    def age_under60():
        name = "age_under60"
        groupings = {
            HHAGE: {
                "Householder age is under 60" : [0, 1, 2, 3, 4]
            }
        }
        return name, groupings

    @staticmethod
    def age_60to64():
        name = "age_60to64"
        groupings = {
            HHAGE: {
                "Householder age is 60 to 64" : [5]
            }
        }
        return name, groupings

    @staticmethod
    def age_65to74():
        name = "age_65to74"
        groupings = {
            HHAGE: {
                "Householder age is 65 to 74" : [6]
            }
        }
        return name, groupings

    @staticmethod
    def age_75plus():
        name = "age_75plus"
        groupings = {
            HHAGE: {
                "Householder age is 75 or older" : [7, 8]
            }
        }
        return name, groupings

    @staticmethod
    def age_65plus():
        name = "age_65plus"
        groupings = {
            HHAGE: {
                "Householder age is 65 or older" : [6, 7, 8]
            }
        }
        return name, groupings


    @staticmethod
    def whiteonly():
        name = "whiteonly"
        groupings = {
            RACE: {
                "White only": [0]
            }
        }
        return name, groupings


    @staticmethod
    def isWhiteAlone():
        name = "isWhiteAlone"
        groupings = {RACE: {"Householder white alone": [0]}}
        return name, groupings

    @staticmethod
    def isBlackAlone():
        name = "isBlackAlone"
        groupings = {RACE: {"Householder black alone": [1]}}
        return name, groupings

    @staticmethod
    def isAIANAlone():
        name = "isAIANAlone"
        groupings = {RACE: {"Householder AIAN alone": [2]}}
        return name, groupings

    @staticmethod
    def isAsianAlone():
        name = "isAsianAlone"
        groupings = {RACE: {"Householder Asian alone": [3]}}
        return name, groupings

    @staticmethod
    def isNHOPIAlone():
        name = "isNHOPIAlone"
        groupings = {RACE: {"Householder NHOPI alone": [4]}}
        return name, groupings

    @staticmethod
    def isSORAlone():
        name = "isSORAlone"
        groupings = {RACE: {"Householder SOR alone": [5]}}
        return name, groupings

    @staticmethod
    def isTMRaces():
        name = "isTMRaces"
        groupings = {RACE: {"Householder is two or more races": [6]}}
        return name, groupings

    @staticmethod
    def yes_multi():
        name = "yes_multi"
        groupings = {
            MULTI: {
                "Yes, multigen": [1]
            }
        }
        return name, groupings

    @staticmethod
    def no_multi():
        name = "no_multi"
        groupings = {
            MULTI: {
                "Not multigen": [0]
            }
        }
        return name, groupings

    @staticmethod
    def family():
        name = "family"
        groupings = {
            HHTYPE: {
                "Family": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14, 15, 16, 19, 20, 21, 22]
            }
        }
        return name, groupings

    @staticmethod
    def nonfamily():
        name = "nonfamily"
        groupings = {
            HHTYPE: {
                "Non-Family": [12, 17, 18, 23]
            }
        }
        return name, groupings

    @staticmethod
    def marriedfamily():
        name = "marriedfamily"
        groupings = {
            HHTYPE: {
                "Married Family": list(range(0,8))
            }
        }
        return name, groupings

    @staticmethod
    def otherfamily():
        name = "otherfamily"
        groupings = {
            HHTYPE: {
                "Other Family": [8, 9, 10, 11, 13, 14, 15, 16, 19, 20, 21, 22]
            }
        }
        return name, groupings

    @staticmethod
    def alone():
        name = "alone"
        groupings = {
            HHTYPE: {
                "Alone": [18]
            }
        }
        return name, groupings

    @staticmethod
    def invert_alone():
        """ everything but living alone """
        name = "invert_alone"
        groupings = {
            HHTYPE: {
                "Not alone": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 19, 20, 21, 22, 23]
            }
        }
        return name, groupings

    @staticmethod
    def notalone():
        """ Nonfamily, not alone """
        name = "notalone"
        groupings = {
            HHTYPE: {
                "Nonfamily, Not alone": [12, 17, 23]
            }
        }
        return name, groupings

    @staticmethod
    def size1():
        name = "size1"
        groupings = {
            SIZE: {
                "1-person household": [1]
            }
        }
        return name, groupings

    @staticmethod
    def vacant():
        name = "vacant"
        groupings = {
            SIZE: {
                "vacant household": [0]
            }
        }
        return name, groupings

    @staticmethod
    def isOccupied():
        name = "isOccupied"
        groupings = {
            SIZE: {
                "Occupied Household": list(range(1,8))
            }
        }
        return name, groupings

    @staticmethod
    def size2plus():
        name = "size2plus"
        groupings = {
            SIZE: {
                "2-or-more-person household": list(range(2,8))
            }
        }
        return name, groupings

    @staticmethod
    def size2():
        name = "size2"
        groupings = {
            SIZE: {
                "2-person household": [2]
            }
        }
        return name, groupings

    @staticmethod
    def non2type():
        name = "non2type"
        groupings = {
            HHTYPE: {
                "Types inconsistent with size 2": [0, 1, 2, 4, 5, 6, 8, 9, 10, 11, 13, 14, 15, 16, 18, 21]
            }
        }
        return name, groupings

    @staticmethod
    def type2_onechild():
        name = "type2_onechild"
        groupings = {
            HHTYPE: {
                "Size 2, householder with child" : [19, 20]
            }
        }
        return name, groupings

    @staticmethod
    def type2_couple():
        name = "type2_couple"
        groupings = {
            HHTYPE: {
                "Size 2, householder with spouse/unmarried partner" : [3, 7, 12, 17]
            }
        }
        return name, groupings

    @staticmethod
    def size3():
        name = "size3"
        groupings = {
            SIZE: {
                "3-person household": [3]
            }
        }
        return name, groupings

    @staticmethod
    def non3type():
        name = "non3type"
        groupings = {
            HHTYPE: {
                "Type not consistent with size 3": [2, 6, 10, 15, 18]
            }
        }
        return name, groupings

    @staticmethod
    def type3_notmulti():
        name = "type3_notmulti"
        groupings = {
            HHTYPE: {
                "Size 3, can't be multi": [0, 1, 3, 4, 5, 7, 8, 9, 11, 12, 13, 14, 16, 17, 21, 23]
            }
        }
        return name, groupings

    @staticmethod
    def type3_twochild():
        name = "type3_twochild"
        groupings = {
            HHTYPE: {
                "Size 3, two children": [21]
            }
        }
        return name, groupings

    @staticmethod
    def type3_couple_with_child():
        name = "type3_couple_with_child"
        groupings = {
            HHTYPE: {
                "Size 3, couple with one child": [0, 1, 4, 5, 8, 9, 13, 14]
            }
        }
        return name, groupings

    @staticmethod
    def size4():
        name = "size4"
        groupings = {
            SIZE: {
                "4-person household" : [4]
            }
        }
        return name, groupings

    @staticmethod
    def type4_notmulti():
        name = "type4_notmulti"
        groupings = {
            HHTYPE: {
                "Size 4, can't be multigen": [2, 6, 10, 12, 15, 17, 23]
            }
        }
        return name, groupings

    @staticmethod
    def type4_couple_with_twochildren():
        name = "type4_couple_with_twochildren"
        groupings = {
             HHTYPE: {
                 "Size 4, couple with two children": [2, 6, 10, 15]
             }
        }
        return name, groupings

    @staticmethod
    def never_multi():
        name = "never_multi"
        groupings = {
            HHTYPE: {
                "Can never be multigen regardless of size" : [12, 17, 18, 23]
            }
        }
        return name, groupings

    def sizex0(self):
        name = "sizex0"
        size_range = range(1,8)
        sizedict = self.getLevels()[SIZE]
        size_groupings = sk.getSubsetTuples(sizedict, size_range)
        groupings = {
            SIZE: dict(size_groupings)
        }
        return name, groupings

    def sizex01(self):
        name = "sizex01"
        size_range = range(2,8)
        sizedict = self.getLevels()[SIZE]
        size_groupings = sk.getSubsetTuples(sizedict, size_range)
        groupings = {
            SIZE: dict(size_groupings)
            # Shouldn't it just be
            # SIZE: {"sizes above 1": size_range}
        }
        return name, groupings

    @staticmethod
    def married_with_children_indicator():
        name = "married_with_children_indicator"
        groupings = {
            HHTYPE: {
                "Married without own children under 18": [3, 7],
                "Married with own children under 18"   : [0, 1, 2, 4, 5, 6]
            }
        }
        return name, groupings

    @staticmethod
    def other_with_children_indicator():
        name = "other_with_children_indicator"
        groupings = {
            HHTYPE: {
                "Other family without own children under 18": [11, 16, 22],
                "Other family with own children under 18"   : [8, 9, 10, 13, 14, 15, 19, 20, 21]
            }
        }
        return name, groupings

    @staticmethod
    def cohabiting():
        name = "cohabiting"
        groupings = {
            HHTYPE: {
                "Cohabiting": list(range(8, 18))
            }
        }
        return name, groupings

    @staticmethod
    def cohabiting_with_children_indicator():
        name = "cohabiting_with_children_indicator"
        groupings = {
            HHTYPE: {
                "Cohabiting without own children under 18" : [11, 12, 16, 17],
                "Cohabiting with own children under 18"    : [8, 9, 10, 13, 14, 15]
            }
        }
        return name, groupings

    @staticmethod
    def no_spouse_or_partner():
        name = "no_spouse_or_partner"
        groupings = {
            HHTYPE: {
                "No spouse or partner": list(range(18, 24))
            }
        }
        return name, groupings

    @staticmethod
    def no_spouse_or_partner_levels():
        name = "no_spouse_or_partner_levels"
        groupings = {
            HHTYPE: {
                "With own children": [19, 20, 21],
                "With relatives, no own children": [22],
                "No relatives, not alone": [23]
            }
        }
        return name, groupings

    @staticmethod
    def presence60():
        name = "presence60"
        groupings = {
            ELDERLY: {
                "No one over 60 years": [0],
                "At least one person over 60 years": [1, 2, 3]
            }
        }
        return name, groupings

    @staticmethod
    def presence65():
        name = "presence65"
        groupings = {
            ELDERLY: {
                "No one over 65 years": [0, 1],
                "At least one person over 65 years": [2, 3]
            }
        }
        return name, groupings

    @staticmethod
    def presence75():
        name = "presence75"
        groupings = {
            ELDERLY: {
                "No one over 75 years": [0, 1, 2],
                "At least one person over 75 years": [3]
            }
        }
        return name, groupings

    @staticmethod
    def married_with_children_levels():
        name = "married_with_children_levels"
        groupings = {
            HHTYPE: {
                "Married with children under 6 only"                   : [0,4],
                "Married with children 6 to 17 years only"             : [1,5],
                "Married with children under 6 years and 6 to 17 years": [2,6]
            }
        }
        return name, groupings


    @staticmethod
    def married_p38_interior():
        name = "married_p38_interior"
        groupings = {
            HHTYPE: {
                "Married with children under 6 only"                   : [0,4],
                "Married with children 6 to 17 years only"             : [1,5],
                "Married with children under 6 years and 6 to 17 years": [2,6],
                "Married no own children under 18"                     : [3,7]
            }
        }
        return name, groupings

    @staticmethod
    def other_with_children_levels():
        name = "other_with_children_levels"
        groupings = {
            HHTYPE: {
                "Other family with children under 6 only"                   : [8,13,19],
                "Other family with children 6 to 17 years only"             : [9,14,20],
                "Other family with children under 6 years and 6 to 17 years": [10,15,21]
            }
        }
        return name, groupings


    @staticmethod
    def other_p38_interior():
        name = "other_p38_interior"
        groupings = {
            HHTYPE: {
                "Other family with children under 6 only"                   : [8,13,19],
                "Other family with children 6 to 17 years only"             : [9,14,20],
                "Other family with children under 6 years and 6 to 17 years": [10,15,21],
                "Other familty no own children"                             : [11,16,22]
            }
        }
        return name, groupings

    @staticmethod
    def couplelevels():
        name = "couplelevels"
        groupings = {
            HHTYPE: {
                "Married": list(range(8)),
                "Unmarried Partner": list(range(8,18)),
                "All others": list(range(18,24))
            }
        }
        return name, groupings

    @staticmethod
    def oppositelevels():
        name = "oppositelevels"
        groupings = {
            HHTYPE: {
                "Married Opposite": list(range(4)),
                "Partner Opposite": list(range(8,13))
            }
        }
        return name, groupings

    @staticmethod
    def samelevels():
        name = "samelevels"
        groupings = {
            HHTYPE: {
                "Married Same": list(range(4,8)),
                "Partner Same": list(range(13,18))
            }
        }
        return name, groupings

    @staticmethod
    def allother():
        name = "allother"
        groupings = {
            HHTYPE: {
                "All others": list(range(18,24))
            }
        }
        return name, groupings

    @staticmethod
    def hhover65():
        name = "hhover65"
        groupings = {
            HHAGE: {
                "15 to 64 years"   : list(range(6)),
                "65 years and over": list(range(6,9))
            }
        }
        return name, groupings

    @staticmethod
    def hhageH18():
        name = "hhageH18"
        groupings = {
            HHAGE: {
                "Householder 15 to 34 years"   : [0,1],
                "Householder 35 to 64 years"   : [2,3,4,5],
                "Householder 65 years and over": [6,7,8]
            }
        }
        return name, groupings

    @staticmethod
    def solo():
        name = "solo"
        groupings = {
            SIZE: {
                "Alone"    : [1],
                "Not alone": list(range(2,8))
            }
        }
        return name, groupings

    @staticmethod
    def isHisp():
        name = "isHisp"
        groupings = {
            HISP: {
                "Householder is Hispanic/Latino"    : [1]
            }
        }
        return name, groupings

    @staticmethod
    def isNotHisp():
        name = "isNotHisp"
        groupings = {
            HISP: {
                "Householder is not Hispanic/Latino"    : [0]
            }
        }
        return name, groupings

HHSEX = SchemaHousehold2010Factory.HHSEX
HHAGE = SchemaHousehold2010Factory.HHAGE
HISP =  SchemaHousehold2010Factory.HISP
RACE =  SchemaHousehold2010Factory.RACE
SIZE =  SchemaHousehold2010Factory.SIZE
HHTYPE = SchemaHousehold2010Factory.HHTYPE
ELDERLY = SchemaHousehold2010Factory.ELDERLY
MULTI = SchemaHousehold2010Factory.MULTI
