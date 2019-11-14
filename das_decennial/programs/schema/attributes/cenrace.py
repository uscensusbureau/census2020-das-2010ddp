from programs.schema.attributes.abstractattribute import AbstractAttribute
import programs.cenrace as cenrace
from constants import CC


class CenraceAttr(AbstractAttribute):

    @staticmethod
    def getName():
        return CC.ATTR_CENRACE

    @staticmethod
    def getLevels():
        return {
                'white'                           : [0],
                'black'                           : [1],
                'aian'                            : [2],
                'asian'                           : [3],
                'nhopi'                           : [4],
                'sor'                             : [5],
                'white-black'                     : [6],
                'white-aian'                      : [7],
                'white-asian'                     : [8],
                'white-nhopi'                     : [9],
                'white-sor'                       : [10],
                'black-aian'                      : [11],
                'black-asian'                     : [12],
                'black-nhopi'                     : [13],
                'black-sor'                       : [14],
                'aian-asian'                      : [15],
                'aian-nhopi'                      : [16],
                'aian-sor'                        : [17],
                'asian-nhopi'                     : [18],
                'asian-sor'                       : [19],
                'nhopi-sor'                       : [20],
                'white-black-aian'                : [21],
                'white-black-asian'               : [22],
                'white-black-nhopi'               : [23],
                'white-black-sor'                 : [24],
                'white-aian-asian'                : [25],
                'white-aian-nhopi'                : [26],
                'white-aian-sor'                  : [27],
                'white-asian-nhopi'               : [28],
                'white-asian-sor'                 : [29],
                'white-nhopi-sor'                 : [30],
                'black-aian-asian'                : [31],
                'black-aian-nhopi'                : [32],
                'black-aian-sor'                  : [33],
                'black-asian-nhopi'               : [34],
                'black-asian-sor'                 : [35],
                'black-nhopi-sor'                 : [36],
                'aian-asian-nhopi'                : [37],
                'aian-asian-sor'                  : [38],
                'aian-nhopi-sor'                  : [39],
                'asian-nhopi-sor'                 : [40],
                'white-black-aian-asian'          : [41],
                'white-black-aian-nhopi'          : [42],
                'white-black-aian-sor'            : [43],
                'white-black-asian-nhopi'         : [44],
                'white-black-asian-sor'           : [45],
                'white-black-nhopi-sor'           : [46],
                'white-aian-asian-nhopi'          : [47],
                'white-aian-asian-sor'            : [48],
                'white-aian-nhopi-sor'            : [49],
                'white-asian-nhopi-sor'           : [50],
                'black-aian-asian-nhopi'          : [51],
                'black-aian-asian-sor'            : [52],
                'black-aian-nhopi-sor'            : [53],
                'black-asian-nhopi-sor'           : [54],
                'aian-asian-nhopi-sor'            : [55],
                'white-black-aian-asian-nhopi'    : [56],
                'white-black-aian-asian-sor'      : [57],
                'white-black-aian-nhopi-sor'      : [58],
                'white-black-asian-nhopi-sor'     : [59],
                'white-aian-asian-nhopi-sor'      : [60],
                'black-aian-asian-nhopi-sor'      : [61],
                'white-black-aian-asian-nhopi-sor': [62]
            }

    @staticmethod
    def recodeNumraces():
        """
        Group CENRACE levels by the number of races present in the level
        """
        name = CC.CENRACE_NUM

        group = cenrace.getCenraceNumberGroups()
        labels = group.getGroupLabels()
        indices = group.getIndexGroups()
        groups =  dict(zip(labels, indices))
        return name, groups

    @staticmethod
    def recodeMajorRaces():
        """
        Group CENRACE into individual single-race categories (alone) and another level that represents all combinations of races
        """
        name = CC.CENRACE_MAJOR
        groups = {
                "White alone": [0],
                "Black or African American alone": [1],
                "American Indian and Alaska Native alone": [2],
                "Asian alone": [3],
                "Native Hawaiian and Other Pacific Islander alone": [4],
                "Some Other Race alone": [5],
                "Two or More Races": list(range(6, 63))
            }
        return name, groups

    @staticmethod
    def recodeRacecomb():
        """

        """
        name = CC.CENRACE_COMB

        groups = cenrace.getCenraceAloneOrInCombinationGroups().getIndexGroups()

        groups = {
                'White alone or in combination with one or more other races': groups[0],
                'Black or African American alone or in combination with one or more other races': groups[1],
                'American Indian and Alaska Native alone or in combination with one or more other races': groups[2],
                'Asian alone or in combination with one or more other races': groups[3],
                'Native Hawaiian and Other Pacific Islander alone or in combination with one or more other races': groups[4],
                'Some Other Race alone or in combination with one or more other races': groups[5]
            }
        return name, groups

    @staticmethod
    def recodeWhiteAlone():
        """
        Subset the CENRACE variable to include only the "White" category, alone
        """
        name = CC.CENRACE_WHITEALONE
        groups = {
                "White alone": [0]
            }
        return name, groups

    @staticmethod
    def recodeBlackAlone():
        """
        Subset the CENRACE variable to include only the "Black or African American" category, alone
        """
        name = CC.CENRACE_BLACKALONE
        groups = {
                "Black or African American alone": [1]
            }
        return name, groups

    @staticmethod
    def recodeAianAlone():
        """
        Subset the CENRACE variable to include only the "American Indian and Alaska Native" category, alone
        """
        name = CC.CENRACE_AIANALONE
        groups = {
                "American Indian and Alaska Native alone": [2]
            }
        return name, groups

    @staticmethod
    def recodeAsianAlone():
        """
        Subset the CENRACE variable to include only the "Asian" category, alone
        """
        name = CC.CENRACE_ASIANALONE
        groups = {
                "Asian alone": [3]
            }
        return name, groups

    @staticmethod
    def recodeNhopiAlone():
        """
        Subset the CENRACE variable to include only the "Native Hawaiian and Pacific Islander" category, alone
        """
        name = CC.CENRACE_NHOPIALONE
        groups = {
                "Native Hawaiian and Pacific Islander alone": [4]
            }
        return name, groups

    @staticmethod
    def recodeSorAlone():
        """
        Subset the CENRACE variable to include only the "Some Other Race" category, alone
        """
        name = CC.CENRACE_SORALONE
        groups = {
            "Some Other Race alone": [5]
            }
        return name, groups

    @staticmethod
    def recodeTomr():
        """
        Subset the CENRACE variable to include only the "Two or more races" category, alone
        """
        name = CC.CENRACE_TOMR
        groups = {
           "Two or more races": list(range(6,63))
            }
        return name, groups
