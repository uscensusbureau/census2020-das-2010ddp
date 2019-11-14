import programs.schema.schema as sk
from programs.schema.schemas.schema_factory import SchemaFactory

HHGQ = "hhgq"


# Stub for backward compatibility
def buildSchema(path=None):
    return SchemaTable10Factory(name="UnitTable10", dimnames=[HHGQ], shape=(30,), path=path).getSchema()

class SchemaTable10Factory(SchemaFactory):

    def addRecodes(self):
        ###############################################################################
        # Recodes for UnitTable10
        ###############################################################################
        self.schema.addRecode(*self.housing_unit())
        self.schema.addRecode(*self.hhgq_vector())

    @staticmethod
    def getLevels():
        leveldict = {
            HHGQ: {
                'Occupied'  : [0],
                'Vacant': [1],
                'Federal detention centers': [2],
                'Federal prisons': [3],
                'State prisons': [4],
                'Local jails and other municipal confinement facilities': [5],
                'Correctional residential facilities': [6],
                'Military disciplinary barracks and jails': [7],
                'Group homes for juveniles (non-correctional)': [8],
                'Residential treatment centers for juveniles (non-correctional)': [9],
                'Correctional facilities intended for juveniles': [10],
                'Nursing facilities/skilled-nursing facilities': [11],
                'Mental (psychiatric) hospitals and psychiatric units in other hospitals': [12],
                'Hospitals with patients who have no usual home elsewhere': [13],
                'In-patient hospice facilities': [14],
                'Military treatment facilities with assigned patients': [15],
                'Residential schools for people with disabilities': [16],
                'College/university student housing': [17],
                'Military quarters': [18],
                'Military ships': [19],
                'Emergency and transitional shelters (with sleeping facilities) for people experiencing homelessness': [20],
                'Soup kitchens': [21],
                'Regularly scheduled mobile food vans': [22],
                'Targeted non-sheltered outdoor locations': [23],
                'Group homes intended for adults': [24],
                'Residential treatment centers for adults': [25],
                'Maritime/merchant vessels': [26],
                'Workers’ group living quarters and job corps centers': [27],
                'Living quarters for victims of natural disasters': [28],
                'Religious group quarters and domestic violence shelters': [29],
            }
        }
        return leveldict


    ###############################################################################
    # Recode functions
    ###############################################################################
    def housing_unit(self):
        name = "housing_unit"
        groupings = {
            HHGQ: {
                "Housing Units" : [0,1]
            }
        }
        return name, groupings

    def hhgq_vector(self):
        name = "hhgq_vector"
        # Make dict like {code: "String description of GQTYPE"}
        reverse_level_dict = {schema_codes[0]: text_value for text_value, schema_codes in self.getLevels()[HHGQ].items()}
        groupings = {
            HHGQ: {
                "Total HU" : [0,1],
            }
        }
        # Add the reverse_level_dict starting from 3rd entry (number 2, after 0 and 1 that are housing units), reversing it back
        groupings[HHGQ].update({reverse_level_dict[schema_code]: [schema_code] for schema_code in range(2, self.schema.shape[self.schema.dimnames.index(HHGQ)])})
        return name, groupings


# This are recodes from CEF/MDF code to our internal schema code for GQTYPE. Used in reader. This and its reverse are temporarily hanging out here,
# until a better place is found (maybe inside the schema or some other class)
reader_recode = {
    "101": 2,
    "102": 3,
    "103": 4,
    "104": 5,
    "105": 6,
    "106": 7,
    "201": 8,
    "202": 9,
    "203": 10,
    "301": 11,
    "401": 12,
    "402": 13,
    "403": 14,
    "404": 15,
    "405": 16,
    "501": 17,
    "601": 18,
    "602": 19,
    "701": 20,
    "702": 21,
    "704": 22,
    "706": 23,
    "801": 24,
    "802": 25,
    "900": 26,
    "901": 27,
    "903": 28,
    "904": 29,
}

# This reverses the recode above, used in MDF2020Writer.
mdf_recode = {(schema_code - 1): mdf_code for mdf_code, schema_code in reader_recode.items()}
