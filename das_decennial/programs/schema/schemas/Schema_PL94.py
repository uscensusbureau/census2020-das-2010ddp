import programs.cenrace as cenrace
import programs.schema.schema as sk
from programs.schema.schemas.schema_factory import SchemaFactory

#from constants import *

# Look for definition of these constants at the bottom of the file, they get values from the SchemaPL94Factory class
# HHGQ = "hhgq"
# VOTING_AGE = "votingage"
# HISP = "hispanic"
# CENRACE = "cenrace"


# Stub for backward compatibility
def buildSchema(path=None):
    return SchemaPL94Factory(name="PL94", dimnames=[HHGQ, VOTING_AGE, HISP, CENRACE], shape=(8, 2, 2, 63), path=path).getSchema()


class SchemaPL94Factory(SchemaFactory):
    HHGQ = "hhgq"
    VOTING_AGE = "votingage"
    HISP = "hispanic"
    CENRACE = "cenrace"

    def addRecodes(self):
        ###############################################################################
        # Recodes for PL94
        ###############################################################################
        self.schema.addRecode(*self.household())
        self.schema.addRecode(*self.gqTotal())
        self.schema.addRecode(*self.gqlevels())
        self.schema.addRecode(*self.institutionalized())
        self.schema.addRecode(*self.numraces())
        self.schema.addRecode(*self.voting())
        self.schema.addRecode(*self.nonvoting())
        self.schema.addRecode(*self.nurse())

    @staticmethod
    def getLevels():
        leveldict = {
            HHGQ: {
                'Household'                              : [0],
                'Correctional Facility (101-106)'        : [1],
                'Juvenile Facility (201-203)'            : [2],
                'Nursing Facility (301)'                 : [3],
                'Other Institutional Facility (401-405)' : [4],
                'College or University Housing (501)'    : [5],
                'Military Quarters (601-602)'            : [6],
                'Other Non-Institutional Facility (>699)': [7]
            },

            VOTING_AGE: {
                'Non-Voting Age': [0],
                'Voting Age'    : [1]
            },

            HISP: {
                'Not Hispanic or Latino': [0],
                'Hispanic or Latino'    : [1]
            },

            CENRACE: {
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
            },
        }

        return leveldict


    ###############################################################################
    # Recode functions
    ###############################################################################
    @staticmethod
    def household():
        name = "household"
        groupings = {
            HHGQ: {
                "Household": [0],
                "Group Quarters": list(range(1,8))
            }
        }
        return name, groupings

    @staticmethod
    def gqTotal():
        name = "gqTotal"
        groupings = {
            HHGQ: {
                "Population in Group Quarters": list(range(1,8))
            }
        }
        return name, groupings


    def gqlevels(self):
        name = "gqlevels"

        gqdict = self.getLevels()[HHGQ]
        gq_range = range(1,8)

        gq_groupings = sk.getSubsetTuples(gqdict, gq_range)

        groupings = {
            HHGQ: dict(gq_groupings)
        }
        return name, groupings

    @staticmethod
    def institutionalized():
        name = "institutionalized"
        groupings = {
            HHGQ: {
                "Institutional Facility"    : list(range(1,5)),
                "Non-institutional Facility": list(range(5,8))
            }
        }
        return name, groupings

    @staticmethod
    def numraces():
        name = 'numraces'
        group = cenrace.getCenraceNumberGroups()
        labels = group.getGroupLabels()
        indices = group.getIndexGroups()
        groupings = {
            CENRACE: dict(zip(labels, indices))
        }
        return name, groupings

    @staticmethod
    def voting():
        """
        Grabs the voting age population only by recoding the VOTING_AGE variable.
        """
        name = 'voting'
        groupings = {
            VOTING_AGE: {
                "Voting Age": [1]
            }
        }
        return name, groupings

    @staticmethod
    def nonvoting():
        name = 'nonvoting'
        groupings = {
            VOTING_AGE: {
                "Non-voting Age": [0]
            }
        }
        return name, groupings

    @staticmethod
    def nurse():
        name = "nurse"
        groupings = {
            HHGQ: {
                "Nursing Homes": [3]
            }
        }
        return name, groupings

HHGQ = SchemaPL94Factory.HHGQ
VOTING_AGE = SchemaPL94Factory.VOTING_AGE
HISP = SchemaPL94Factory.HISP
CENRACE = SchemaPL94Factory.CENRACE