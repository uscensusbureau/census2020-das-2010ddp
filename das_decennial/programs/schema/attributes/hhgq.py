from programs.schema.attributes.abstractattribute import AbstractAttribute
from constants import CC


class HHGQAttr(AbstractAttribute):

    @staticmethod
    def getName():
        return CC.ATTR_HHGQ

    @staticmethod
    def getLevels():
        return {
                    "Household"                                    : [0],
                    "Correctional facilities for adults"           : [1],
                    "Juvenile facilities"                          : [2],
                    "Nursing facilities/Skilled-nursing facilities": [3],
                    "Other institutional facilities"               : [4],
                    "College/University student housing"           : [5],
                    "Military quarters"                            : [6],
                    "Other noninstitutional facilities"            : [7]
                }

    @staticmethod
    def recodeHouseholdTotal():
        name = CC.HHGQ_HOUSEHOLD_TOTAL
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
        name = CC.HHGQ_GQLEVELS
        groups = {
                    "Correctional facilities for adults"           : [1],
                    "Juvenile facilities"                          : [2],
                    "Nursing facilities/Skilled-nursing facilities": [3],
                    "Other institutional facilities"               : [4],
                    "College/University student housing"           : [5],
                    "Military quarters"                            : [6],
                    "Other noninstitutional facilities"            : [7]
                }

        return name, groups

    @staticmethod
    def recodeInstLevels():
        """
        Groups the HHGQ variable's GQ types into Institutional and Non-Institutional
        """
        name = CC.HHGQ_INSTLEVELS

        groups = {
                "Institutional Facility": list(range(1, 5)),
                "Non-Institutional Facility": list(range(5, 8))
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
                "Institutional Population Total": list(range(1, 5))
            }
        return name, groups

    @staticmethod
    def recodeGqCorrectionalTotal():
        """
        Grabs the subset of the HHGQ variable corresponding to correctional facilities for adults and calculates the total
        """
        name = CC.HHGQ_CORRECTIONAL_TOTAL
        groups = {
                "Correctional Facilities for Adults Population Total": [1]
            }
        return name, groups

    @staticmethod
    def recodeGqJuvenileTotal():
        """
        Grabs the subset of the HHGQ variable corresponding to juvenile facilities and calculates the total
        """
        name = CC.HHGQ_JUVENILE_TOTAL
        groups = {
                "Juvenile Facilities Population Total": [2]
            }
        return name, groups

    @staticmethod
    def recodeGqNursingTotal():
        """
        Grabs the subset of the HHGQ variable corresponding to nursing facilities and calculates the total
        """
        name = CC.HHGQ_NURSING_TOTAL
        groups = {
                "Nursing Facilities Population Total": [3]
            }
        return name, groups

    @staticmethod
    def recodeGqOtherInstTotal():
        """
        Grabs the subset of the HHGQ variable corresponding to other institutional facilities and calculates the total
        """
        name = CC.HHGQ_OTHERINST_TOTAL
        groups = {
                "Other Institutional Facilities Population Total": [4]
            }
        return name, groups

    @staticmethod
    def recodeNoninstTotal():
        """
        Query for calculating the total of the 'noninstitutionalized population' from the HHGQ variable
        """
        name = CC.HHGQ_NONINST_TOTAL
        groups = {
                "Noninstitutionalized Population Total": list(range(5, 8))
            }
        return name, groups

    @staticmethod
    def recodeGqCollegeTotal():
        """
        Grabs the subset of the HHGQ variable corresponding to college/university student housing levels and calculates the total

        Note: GQ 502 is missing from the histogram... is this intentional or a typo?
        """
        name = CC.HHGQ_COLLEGE_TOTAL
        groups = {
                "College/University Student Housing Population Total": [5]
            }
        return name, groups

    @staticmethod
    def recodeGqMilitaryTotal():
        """
        Grabs the subset of the HHGQ variable corresponding to military quarters housing levels and calculates the total
        """
        name = CC.HHGQ_MILITARY_TOTAL
        groups = {
                "Military Quarters Housing Population Total": [6]
            }
        return name, groups

    @staticmethod
    def recodeGqOtherNoninstTotal():
        """
        Grabs the subset of the HHGQ variable corresponding to other noninstitutional facilities and calculates the total
        """
        name = CC.HHGQ_OTHERNONINST_TOTAL
        groups = {
                "Other Noninstitutional Facilities Population Total": [7]
            }
        return name, groups
