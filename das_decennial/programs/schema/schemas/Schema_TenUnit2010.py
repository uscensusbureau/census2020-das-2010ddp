from programs.schema.schemas.Schema_Household2010 import HHAGE, HHSEX, HHTYPE, HISP, RACE, SIZE, ELDERLY, MULTI, SchemaHousehold2010Factory

TENURE = "tenure"

# Stub for backward compatibility
def buildSchema(path=None):
    return SchemaHousehold2010Factory(name="TenUnit2010",
                                      dimnames=[HHSEX, HHAGE, HISP, RACE, SIZE, HHTYPE, ELDERLY, MULTI, TENURE],
                                      shape=(2, 9, 2, 7, 8, 24, 4, 2, 4), path=path).getSchema()


class SchemaTenUnit2010Factory(SchemaHousehold2010Factory):

    @staticmethod
    def getLevels():
        leveldict = SchemaHousehold2010Factory.getLevels()
        leveldict.update(
            {
                TENURE: {
                    "Mortgage": [0],
                    "Owned": [1],
                    "Rented": [2],
                    "No Pay": [3]
                }
            }
        )
        return leveldict