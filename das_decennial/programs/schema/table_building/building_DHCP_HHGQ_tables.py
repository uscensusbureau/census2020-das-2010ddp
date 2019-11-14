
import programs.schema.schemas.Schema_DHCP_HHGQ as myschema
import programs.schema.table_building.tablebuilder as tablebuilder
import numpy as np
import pandas as pd
import das_utils
import programs.cenrace as cenrace

def getTableDict():
    tabledict = {
        ################################################
        # Hierarchical age category tables
        ################################################
        # Prefix Query
        "prefixquery": ['prefix_agecats', 'age'],

        # Range Query
        "rangequery": ['range_agecats', 'age'],

        # Prefix Query
        "binarysplitquery": ['binarysplit_agecats', 'age'],

        ################################################
        # SF1 Proposed Tables for 2020 (Person-focused)
        ################################################

        # Table P1 - Total Population
        # Universe: Total Population
        "P1": ["total"],

        # Table P3 - Race
        # Universe: Total Population
        "P3": ["total", "majorRaces"],

        # Table P4 - Hispanic or Latino
        # Universe: Total Population
        "P4": ["total", "hispanic"],

        # Table P5 - Hispanic or Latino by Race
        # Universe: Total Population
        "P5": ["total", "hispanic", "hispanic * majorRaces"],

        # Table P6 - Race (Total Races Tallied)
        # Universe: Total Races Tallied ???
        # What does the total races tallied query mean??? Do we use "total" or some
        # recode that totals the racecomb query answers?
        "P6": [# "racecombTotal",
            # "total",
            "racecomb"],

        # Table P7 - Hispanic or Latino by Race (Total Races Tallied)
        # Universe: Total Races Tallied ???
        # What does the total races tallied query mean??? Do we use "total" or some
        # recode that totals the racecomb query answers?
        "P7": [# "racecombTotal",
            # "total",
            # "hispanic * racecombTotal",
            # "hispanic",
            "racecomb", "hispanic * racecomb"],

        # Table P12 - Sex by Age
        # Universe: Total Population
        "P12": ["total", "sex", "sex * agecat"],

        # Table P13 - Median age by Sex (1 Expressed Decimal)

        # Table P14 - Sex by Age for the Population Under 20 Years
        # Universe: Population Under 20 Years
        "P14": ["under20yearsTotal", "under20yearsTotal * sex", "under20years * sex"],

        # Table P16 - Population in households by age
        "P16": ["hhTotal", "hhTotal * votingage"],

        # Table P43 - GQ Population by Sex by Age by Major GQ Type
        # Universe: Population in GQs
        "P43": ["gqTotal", "gqTotal * sex", "gqTotal * sex * agecat43",  "sex * agecat43 * institutionalized",
                "sex * agecat43 * majorGQs"],


        # Table P12A - Sex by Age (White alone)
        # Universe: People who are White alone
        "P12A": [
            "whiteAlone", "whiteAlone * sex", "whiteAlone * sex * agecat",
        ],

        # Table P12B - Sex by Age (Black or African American alone)
        # Universe: People who are Black or African American alone
        "P12B": [
            "blackAlone", "blackAlone * sex", "blackAlone * sex * agecat"
        ],

        # Table P12C - Sex by Age (American Indian and Alaska Native alone)
        # Universe: People who are American Indian and Alaska Native alone
        "P12C": [
            "aianAlone", "aianAlone * sex", "aianAlone * sex * agecat"
        ],

        # Table P12D - Sex by Age (Asian alone)
        # Universe: People who are Asian alone
        "P12D": [
            "asianAlone", "asianAlone * sex", "asianAlone * sex * agecat"
        ],

        # Table P12E - Sex by Age (Native Hawaiian and Other Pacific Islander alone)
        # Universe: People who are Native Hawaiian and Other Pacific Islander alone)
        "P12E": [
            "nhopiAlone",  "nhopiAlone * sex", "nhopiAlone * sex * agecat"
        ],

        # Table P12F - Sex by Age (Some Other Race alone)
        # Universe: People who are Some Other Race alone
        "P12F": [
            "sorAlone", "sorAlone * sex", "sorAlone * sex * agecat"
        ],

        # Table P12G - Sex by Age (Two or more races)
        # Universe: People who are two or more races
        "P12G": [
            "tomr", "tomr * sex", "tomr * sex * agecat"
        ],

        # Table P12H - Sex by Age (Hispanic or Latino)
        # Universe: People who are Hispanic or Latino
        "P12H": [
            "hispTotal", "hispTotal * sex", "hispTotal * sex * agecat"
        ],

        # Table P12I - Sex by Age (White alone, not Hispanic or Latino)
        "P12I": [
            "whiteAlone * notHispTotal", "whiteAlone * notHispTotal * sex", "whiteAlone * notHispTotal * sex * agecat"
        ],

        # Tables P13A-I Median age by sex

        # PCO1 - Group Quarters Population by Sex by Age
        # Universe: Population in group quarters
        "PCO1": ["gqTotal", "gqTotal * sex", "gqTotal * sex * agecatPCO1"],

        # PCO2 - Group Quarters Population in Institutional Facilities by Sex by Age
        # Universe: Institutionalized Population
        "PCO2": ["instTotal", "instTotal * sex", "instTotal * sex * agecatPCO1"],

        # PCO3 - GQ Pop in Correctional Facilities for Adults by Sex by Age
        # Universe: Pop in correctional facilities for adults
        "PCO3": ["gqCorrectionalTotal", "gqCorrectionalTotal * sex",  "gqCorrectionalTotal * sex * agecatPCO3"],

        # PCO4 - GQ Pop in Juvenile Facilities by Sex by Age
        # Universe: Pop in juvenile facilities
        "PCO4": ["gqJuvenileTotal", "gqJuvenileTotal * sex", "gqJuvenileTotal * sex * agecatPCO4"],

        # PCO5 - GQ Pop in Nursing Facilities / Skilled-Nursing Facilities by Sex by Age
        # Universe: Pop in nursing facilities/skilled-nursing facilities
        "PCO5": ["gqNursingTotal", "gqNursingTotal * sex", "gqNursingTotal * sex * agecatPCO5"],

        # PCO6 - GQ Pop in Other Institutional Facilities by Sex by Age
        # Universe: Pop in other institutional facilities
        "PCO6": ["gqOtherInstTotal", "gqOtherInstTotal * sex", "gqOtherInstTotal * sex * agecatPCO1"],

        # PCO7 - GQ Pop in Noninstitutional Facilities by Sex by Age
        # Universe: Pop in noninstitutional facilities
        "PCO7": ["noninstTotal", "noninstTotal * sex", "noninstTotal * sex * agecatPCO7"],

        # PCO8 - GQ Pop in College/University Student Housing by Sex by Age
        # Universe: Pop in college/university student housing
        "PCO8": ["gqCollegeTotal", "gqCollegeTotal * sex", "gqCollegeTotal * sex * agecatPCO8"],

        # PCO9 - GQ Pop in Military Quarters by Sex by Age
        # Universe: Pop in military quarters
        "PCO9": ["gqMilitaryTotal", "gqMilitaryTotal * sex",  "gqMilitaryTotal * sex * agecatPCO8"],

        # PCO10 - GQ Pop in Other Noninstitutional Facilities by Sex by Age
        # Universe: Pop in other noninstitutional facilities
        "PCO10": ["gqOtherNoninstTotal", "gqOtherNoninstTotal * sex", "gqOtherNoninstTotal * sex * agecatPCO7"],

        # PCO43A - GQ Pop by Sex by Age by Major GQ Type (White alone)
        # Universe: Pop in group quarters
         "PCO43A": [
             "whiteAlone * gqTotal",
             "whiteAlone * gqTotal * sex",
             "whiteAlone * gqTotal * sex * agecat43",
             "whiteAlone * institutionalized * sex * agecat43",
             "whiteAlone * majorGQs * sex * agecat43"
         ],

        # PCO43B - GQ Pop by Sex by Age by Major GQ Type (Black or African American alone)
        # Universe: Pop in group quarters
         "PCO43B": [
             "blackAlone * gqTotal",
             "blackAlone * gqTotal * sex",
             "blackAlone * gqTotal * sex * agecat43",
             "blackAlone * institutionalized * sex * agecat43",
             "blackAlone * majorGQs * sex * agecat43"
         ],

        # PCO43C - GQ Pop by Sex by Age by Major GQ Type (American Indian or Alaska Native alone)
        # Universe: Pop in group quarters
         "PCO43C": [
             "aianAlone * gqTotal",
             "aianAlone * gqTotal * sex",
             "aianAlone * gqTotal * sex * agecat43",
             "aianAlone * institutionalized * sex * agecat43",
             "aianAlone * majorGQs * sex * agecat43"
         ],

        # PCO43D - GQ Pop by Sex by Age by Major GQ Type (Asian alone)
        # Universe: Pop in group quarters
         "PCO43D": [
             "asianAlone * gqTotal",
             "asianAlone * gqTotal * sex",
             "asianAlone * gqTotal * sex * agecat43",
             "asianAlone * institutionalized * sex * agecat43",
             "asianAlone * majorGQs * sex * agecat43"
         ],

        # PCO43E - GQ Pop by Sex by Age by Major GQ Type (Native Hawaiian or Other Pacific Islander alone)
        # Universe: Pop in group quarters
         "PCO43E": [
             "nhopiAlone * gqTotal",
             "nhopiAlone * gqTotal * sex",
             "nhopiAlone * gqTotal * sex * agecat43",
             "nhopiAlone * institutionalized * sex * agecat43",
             "nhopiAlone * majorGQs * sex * agecat43"
         ],

        # PCO43F - GQ Pop by Sex by Age by Major GQ Type (Some Other Race alone)
        # Universe: Pop in group quarters
         "PCO43F": [
             "sorAlone * gqTotal",
             "sorAlone * gqTotal * sex",
             "sorAlone * gqTotal * sex * agecat43",
             "sorAlone * institutionalized * sex * agecat43",
             "sorAlone * majorGQs * sex * agecat43"
         ],

        # PCO43G - GQ Pop by Sex by Age by Major GQ Type (Two or More Races Alone)
        # Universe: Pop in group quarters
         "PCO43G": [
             "tomr * gqTotal",
             "tomr * gqTotal * sex",
             "tomr * gqTotal * sex * agecat43",
             "tomr * institutionalized * sex * agecat43",
             "tomr * majorGQs * sex * agecat43"
         ],

        # PCO43H - GQ Pop by Sex by Age by Major GQ Type (Hispanic or Latino)
        # Universe: Pop in group quarters
         "PCO43H": [
             "hispTotal * gqTotal",
             "hispTotal * gqTotal * sex",
             "hispTotal * gqTotal * sex * agecat43",
             "hispTotal * institutionalized * sex * agecat43",
             "hispTotal * majorGQs * sex * agecat43"
         ],

        # PCO43I - GQ Pop by Sex by Age by Major GQ Type (White Alone, Not Hispanic or Latino)
        # Universe: Pop in group quarters
        ### The [complement/partition of each race * not hispanic] of the universe might be useful ####
         "PCO43I": [
             "whiteAlone * notHispTotal * gqTotal",
             "whiteAlone * notHispTotal * gqTotal * sex",
             "whiteAlone * notHispTotal * gqTotal * sex * agecat43",
             "whiteAlone * notHispTotal * institutionalized * sex * agecat43",
             "whiteAlone * notHispTotal * majorGQs * sex * agecat43"
         ],

        # PCT12 - Sex by Age
        # Universe: Total Population
        "PCT12": ["total", "sex", "sex * agecatPCT12"],

        # PCT13
        # Universe: Population in households
        "PCT13": ["hhTotal", "hhTotal * sex", "hhTotal * sex * agecat"],

        # PCT22 - GQ Pop by Sex by Major GQ Type for Pop 18 Years and Over
        # Universe: Pop 18 years and over in group quarters
        "PCT22": ["over17yearsTotal * gqTotal", "over17yearsTotal * gqTotal * sex", "over17yearsTotal * sex * institutionalized",
                  "over17yearsTotal * sex * majorGQs"],

        # PCT13A
        # Universe: Population of White alone  in households
        "PCT13A": [
            "whiteAlone * hhTotal", "whiteAlone * hhTotal * sex", "whiteAlone * hhTotal * sex * agecat",
        ],

        # PCT13B
        # Universe: Population of Black alone in households
        "PCT13B": ["blackAlone * hhTotal", "blackAlone * hhTotal * sex", "blackAlone * hhTotal * sex * agecat",
        ],

        # PCT13C
        # Universe: Population of AIAN alone  in households
        "PCT13C": ["aianAlone * hhTotal", "aianAlone * hhTotal * sex", "aianAlone * hhTotal * sex * agecat",
        ],

        # PCT13D
        # Universe: Population of Asian alone  in households
        "PCT13D": ["asianAlone * hhTotal", "asianAlone * hhTotal * sex", "asianAlone * hhTotal * sex * agecat",
        ],

        # PCT13E
        # Universe: Population of Hawaiian/Pacific Islander alone  in households
        "PCT13E": ["nhopiAlone * hhTotal", "nhopiAlone * hhTotal * sex", "nhopiAlone * hhTotal * sex * agecat",
        ],

        # PCT13F
        # Universe: Population of Some other race alone  in households
        "PCT13F": ["sorAlone * hhTotal", "sorAlone * hhTotal * sex", "sorAlone * hhTotal * sex * agecat",
        ],

        # PCT13G
        # Universe: Population of Two or more races   in households
        "PCT13G": ["tomr * hhTotal", "tomr * hhTotal * sex", "tomr * hhTotal * sex * agecat",
        ],

        # PCT13H
        # Universe: Population of Hispanic or latino in households
        "PCT13H": ["hispTotal * hhTotal", "hispTotal * hhTotal * sex", "hispTotal * hhTotal * sex * agecat",
        ],

        # PCT13I
        # Universe: Population of White alone, non-hispanic in households
        "PCT13I": ["whiteAlone * notHispTotal * hhTotal", "whiteAlone * notHispTotal * hhTotal * sex",
            "whiteAlone * notHispTotal * hhTotal * sex * agecat"
        ],

    }

    return tabledict

def getTableBuilder(testtables=None):
    """
    :param testtables: Will run the testTableDefs function to ensure recodes
     from the same dimension aren't crossed in a table. This is useful if you
     get the error because it identifies the table and the cell of the list where
     the error occurs.

    :return:
    """
    schema = myschema.buildSchema()
    tabledict = getTableDict()
    if testtables==True:

        tablebuilder.testTableDefs(schema, tabledict)

    else:
        schema = myschema.buildSchema()
        tabledict = getTableDict()
        builder = tablebuilder.TableBuilder(schema, tabledict)
        return builder


############################################################
## Consolidated tables
############################################################

        '''
        # Tables P12A-I
        # Combines individual P12A-I tables
        "P12A-I": [# P12A-G
            "total", "majorRaces", "majorRaces * sex", "majorRaces * sex * agecat",

            # P12H
            "hispTotal", "hispTotal * sex", "hispTotal * sex * agecat",

            # P12I
            "whiteAlone * notHispTotal", "whiteAlone * notHispTotal * sex", "whiteAlone * notHispTotal * sex * agecat"],
        
        # Tables PCO43A-I
        # Combines individual PCO43A-I tables
        "PCO43A-I": [
            # PCO43A-G
            "majorRaces * gqTotal", "majorRaces * gqTotal * sex", "majorRaces * gqTotal * sex * agecat43",
            "majorRaces * sex * agecat43 * institutionalized", "majorRaces * sex * agecat43 * majorGQs",

            # PCO43H
            "hispTotal * gqTotal", "hispTotal * gqTotal * sex", "hispTotal * gqTotal * sex * agecat43",
            "hispTotal * sex * agecat43 * institutionalized", "hispTotal * sex * agecat43 * majorGQs",

            # PCO43I
            "whiteAlone * notHispTotal * gqTotal", "hispTotal * gqTotal * sex", "whiteAlone * notHispTotal * gqTotal * sex * agecat43",
            "whiteAlone * notHispTotal * sex * agecat43 * institutionalized", "whiteAlone * notHispTotal * sex * agecat43 * majorGQs"
        ],
        
        # PCT13A-I
        #Combines individual tables PCT13A-PCT13I
        #Universe: Population in households for major race group, hispanic, or white along/non-hispanic
        "PCT13A-I": [
            # PCT13A-G
            "majorRaces * hhTotal", "majorRaces * hhTotal * sex", "majorRaces * hhTotal * sex * agecat",

            # PCT13H
            "hispTotal * hhTotal", "hispTotal * hhTotal * sex", "hispTotal * hhTotal * sex * agecat",

            #PCT13I
            "whiteAlone * notHispTotal * hhTotal", "whiteAlone * notHispTotal * hhTotal * sex", "whiteAlone * notHispTotal * hhTotal * sex * agecat"
        ],
        
        
        
        
        
        
        
        
        
        '''