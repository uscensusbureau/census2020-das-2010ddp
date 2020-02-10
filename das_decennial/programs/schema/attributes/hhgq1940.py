from programs.schema.attributes.abstractattribute import AbstractAttribute
from constants import CC


class HHGQ1940Attr(AbstractAttribute):

    @staticmethod
    def getName():
        return CC.ATTR_HHGQ_1940

    @staticmethod
    def getLevels():
        return {
            "Household"                                          : [0],
            "Correctional Institutions"                          : [1],
            "Mental Institutions"                                : [2],
            "Institutions for the Elderly, Handicapped, and Poor": [3],
            "Military"                                           : [4],
            "College Dormitory"                                  : [5],
            "Rooming House"                                      : [6],
            "Other Non-Institutional GQ and Unknown"             : [7]
        }

    @staticmethod
    def recodeHouseholdTotal():
        name = CC.HHGQ_1940_HOUSEHOLD_TOTAL
        groups = {
                "Household": [0],
            }
        return name, groups

    @staticmethod
    def recodeHhgq2level():
        """
        collapses the HHGQ dimension to 2 levels (household and group quarters) by
        grouping together the 7 group quarter types)
        """
        name = CC.HHGQ_2LEVEL
        groups = {
                "Household": [0],
                "Group Quarters": list(range(1, 8))
            }
        return name, groups

    @staticmethod
    def recodeGqlevels():
        """
        Only the GQ levels, without the household
        """
        name = CC.HHGQ_1940_GQLEVELS
        groups = {k: v for k, v in HHGQ1940Attr.getLevels().items() if v[0] > 0}

        return name, groups

    @staticmethod
    def recodeInstLevels():
        """
        Groups the HHGQ variable's GQ types into Institutional and Non-Institutional
        """
        name = CC.HHGQ_1940_INSTLEVELS

        groups = {
                "Institutional Facility": list(range(1, 4)),
                "Non-Institutional Facility": list(range(4, 8))
            }
        return name, groups

    @staticmethod
    def recodeGqTotal():
        """
        Query that gives the number of persons in GQs
        """
        name = CC.HHGQ_GQTOTAL

        groups = {
                "Total Population in Group Quarters": list(range(1, 8))
        }
        return name, groups

    @staticmethod
    def recodeInstTotal():
        """
        Query for calculating the total of the 'institutionalized population' from the HHGQ variable
        """
        name = CC.HHGQ_INST_TOTAL
        groups = {
                "Institutional Population Total": list(range(1, 4))
            }
        return name, groups

    @staticmethod
    def recodeGqCorrectionalTotal():
        """
        Grabs the subset of the HHGQ variable corresponding to correctional institutions and calculates the total
        """
        name = CC.HHGQ_CORRECTIONAL_TOTAL
        groups = {
                "Correctional Institutions Total": [1]
            }
        return name, groups

    @staticmethod
    def recodeGqMentalTotal():
        """
        Grabs the subset of the HHGQ variable corresponding to mental institutions and calculates the total
        """
        name = CC.HHGQ_1940_MENTAL_TOTAL
        groups = {
                "Mental Institutions Total": [2]
            }
        return name, groups

    @staticmethod
    def recodeGqElderlyTotal():
        """
        Grabs the subset of the HHGQ variable corresponding to institutions for the elderly, handicapped, and poor and calculates the total
        """
        name = CC.HHGQ_1940_ELDERLY_TOTAL
        groups = {
                "Institutions for the Elderly, Handicapped, and Poor Total": [3]
            }
        return name, groups

    @staticmethod
    def recodeNoninstTotal():
        """
        Query for calculating the total of the 'noninstitutionalized population' from the HHGQ variable
        """
        name = CC.HHGQ_NONINST_TOTAL
        groups = {
                "Noninstitutionalized Population Total": list(range(4, 8))
            }
        return name, groups

    @staticmethod
    def recodeGqMilitaryTotal():
        """
        Grabs the subset of the HHGQ1940 variable corresponding to military level and calculates the total
        """
        name = CC.HHGQ_MILITARY_TOTAL
        groups = {
                "Military Total": [4]
            }
        return name, groups

    @staticmethod
    def recodeGqCollegeTotal():
        """
        Grabs the subset of the HHGQ1940 variable corresponding to the college dormitory level and calculates the total
        """
        name = CC.HHGQ_COLLEGE_TOTAL
        groups = {
                "College Dormitory Total": [5]
            }
        return name, groups

    @staticmethod
    def recodeGqRoomingTotal():
        """
        Grabs the subset of the HHGQ1940 variable corresponding to rooming house level and calculates the total
        """
        name = CC.HHGQ_1940_ROOMING_TOTAL
        groups = {
                "Rooming House Total": [6]
            }
        return name, groups

    @staticmethod
    def recodeGqOtherNoninstTotal():
        """
        Grabs the subset of the HHGQ variable corresponding to other noninstitutional gq and unknown level and calculates the total
        """
        name = CC.HHGQ_OTHERNONINST_TOTAL
        groups = {
                "Other Non-Institutional GQ and Unknown Total": [7]
            }
        return name, groups
