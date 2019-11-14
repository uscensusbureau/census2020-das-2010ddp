import programs.cenrace as cenrace
import programs.schema.schema as sk

from programs.schema.schemas.Schema_PL94 import SchemaPL94Factory

#from constants import *

## Look for definition of these constants at the bottom of the file, they get values from the SchemaPL94P12Factory class
# HHGQ = SchemaPL94P12Factory.HHGQ
# HISP = SchemaPL94P12Factory.HISP
# CENRACE = SchemaPL94P12Factory.CENRACE
# AGECAT = SchemaPL94P12Factory.AGECAT
# SEX = SchemaPL94P12Factory.SEX


# Stub for backward compatibility
def buildSchema(path=None):
    return SchemaPL94P12Factory(name="PL94_P12", dimnames=[HHGQ, SEX, AGECAT, HISP, CENRACE], shape=(8, 2, 23, 2, 63), path=path).getSchema()


class SchemaPL94P12Factory(SchemaPL94Factory):
    SEX = "sex"
    AGECAT = "agecat"

    def addRecodes(self):

        ###############################################################################
        # Recodes for PL94_P12
        ###############################################################################
        self.schema.addRecode(*self.household())
        self.schema.addRecode(*self.gqlevels())
        self.schema.addRecode(*self.institutionalized())
        self.schema.addRecode(*self.numraces())
        self.schema.addRecode(*self.votingage())
        self.schema.addRecode(*self.voting())
        self.schema.addRecode(*self.nonvoting())
        self.schema.addRecode(*self.nurse())

    @staticmethod
    def getLevels():
        leveldict_pl94 = SchemaPL94Factory.getLevels()
        leveldict = {var: leveldict_pl94[var] for var in (HHGQ, HISP, CENRACE)}
        leveldict.update(
            {
                SEX: {
                    'Male'  : [0],
                    'Female': [1]
                },

                AGECAT: {
                    'Under 5 years'    : [0],
                    '5 to 9 years'     : [1],
                    '10 to 14 years'   : [2],
                    '15 to 17 years'   : [3],
                    '18 to 19 years'   : [4],
                    '20 to 21 years'   : [5],
                    '22 to 24 years'   : [6],
                    '25 to 29 years'   : [7],
                    '30 to 34 years'   : [8],
                    '35 to 39 years'   : [9],
                    '40 to 44 years'   : [10],
                    '45 to 49 years'   : [11],
                    '50 to 54 years'   : [12],
                    '55 to 59 years'   : [13],
                    '60 to 61 years'   : [14],
                    '62 to 64 years'   : [15],
                    '65 years'         : [16],
                    '66 years'         : [17],
                    '67 to 69 years'   : [18],
                    '70 to 74 years'   : [19],
                    '75 to 79 years'   : [20],
                    '80 to 84 years'   : [21],
                    '85 years and over': [22]
                },
            }
        )

        return leveldict


    ###############################################################################
    # Recode functions
    ###############################################################################
    @staticmethod
    def votingage():
        name = "votingage"
        groupings = {
            AGECAT: {
                "Non-voting age": [0,1,2,3],
                "Voting age"    : list(range(4,23))
            }
        }
        return name, groupings

    @staticmethod
    def voting():
        name = 'voting'
        groupings = {
            AGECAT: {
                "Voting Age": list(range(4,23))
            }
        }
        return name, groupings

    @staticmethod
    def nonvoting():
        name = 'nonvoting'
        groupings = {
            AGECAT: {
                "Non-voting Age": [0,1,2,3]
            }
        }
        return name, groupings


HHGQ = SchemaPL94P12Factory.HHGQ
HISP = SchemaPL94P12Factory.HISP
CENRACE = SchemaPL94P12Factory.CENRACE
AGECAT = SchemaPL94P12Factory.AGECAT
SEX = SchemaPL94P12Factory.SEX
