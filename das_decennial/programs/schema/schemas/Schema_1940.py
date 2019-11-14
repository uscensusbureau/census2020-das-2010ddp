from programs.schema.schemas.Schema_PL94 import SchemaPL94Factory

# from constants import *

# Look for definition of these constants at the bottom of the file, they get values from the Schema1940Factory class
# HISP_GROUP = "hispanicgroup"
# CITIZEN = "citizen"
# CITIZEN_GROUP = "citizengroup"
# RACE = "race"


# Stub for backward compatibility
def buildSchema(path=None):
    return Schema1940Factory(name="Census1940", dimnames=[HHGQ, VOTING_AGE, HISP_GROUP, CITIZEN_GROUP, RACE], shape=(8, 2, 5, 5, 6), path=path).getSchema()


class Schema1940Factory(SchemaPL94Factory):
    HISP_GROUP = "hispanicgroup"
    CITIZEN = "citizen"
    CITIZEN_GROUP = "citizengroup"
    RACE = "race"

    def addRecodes(self):
        ###############################################################################
        ### Recodes for 1940
        ##############################################################################


        self.schema.addRecode(*self.household())
        self.schema.addRecode(*self.gqTotal())
        self.schema.addRecode(*self.gqlevels())
        self.schema.addRecode(*self.institutionalized())
        self.schema.addRecode(*self.voting())
        self.schema.addRecode(*self.hispanic())
        self.schema.addRecode(*self.citizen())

    @staticmethod
    def getLevels():
        leveldict = {
            HHGQ: {
                'Household'                                          : [0],
                'Correctional Institutions'                          : [1],
                'Mental Institutions'                                : [2],
                'Institutions for the Elderly, Handicapped, and Poor': [3],
                'Military'                                           : [4],
                'College Dormitory'                                  : [5],
                'Rooming House'                                      : [6],
                'Other Non-Institutional GQ and Unknown'             : [7]
            },

            VOTING_AGE: {
                '17 and under': [0],
                '18 and over' : [1]
            },

            HISP_GROUP: {
                'Not Hispanic': [0],
                'Mexican'     : [1],
                'Puerto Rican': [2],
                'Cuban'       : [3],
                'Other'       : [4]
            },

            CITIZEN_GROUP: {
                'N/A'                                         : [0],
                'Born abroad of American parents'             : [1],
                'Naturalized Citizen'                         : [2],
                'Not a citizen'                               : [3],
                'Not a citizen, but has received first papers': [4]
            },

            RACE: {
                'White'                           : [0],
                'Black/African American'          : [1],
                'American Indian or Alaska Native': [2],
                'Chinese'                         : [3],
                'Japanese'                        : [4],
                'Other Asian or Pacific Islander' : [5]
            },

        }

        return leveldict

    ###############################################################################
    # Recode functions
    ###############################################################################

    @staticmethod
    def hispanic():
        """
        Makes not hispanic/hispanic by recoding the HISP_GROUP variable.
        """
        name = HISP
        groupings = {
            HISP_GROUP: {
                'Not Hispanic or Latino': [0],
                'Hispanic or Latino'    : [1,2,3,4]
            }
        }
        return name, groupings

    @staticmethod
    def citizen():
        """
        Makes not citizen binaryby recoding the HISP_GROUP variable.
        """
        name = CITIZEN
        groupings = {
            CITIZEN_GROUP: {
                'Citizen'    : [0,1,2],
                'Not a Citizen': [3,4]
            }
        }
        return name, groupings

    @staticmethod
    def institutionalized():
        name = "institutionalized"
        groupings = {
            HHGQ: {
                "Institutional Facility"    : list(range(1,4)),
                "Non-institutional Facility": list(range(4,8))
            }
        }
        return name, groupings


HHGQ = Schema1940Factory.HHGQ
VOTING_AGE = Schema1940Factory.VOTING_AGE
HISP = Schema1940Factory.HISP
HISP_GROUP = Schema1940Factory.HISP_GROUP
CITIZEN = Schema1940Factory.CITIZEN
CITIZEN_GROUP = Schema1940Factory.CITIZEN_GROUP
RACE = Schema1940Factory.RACE