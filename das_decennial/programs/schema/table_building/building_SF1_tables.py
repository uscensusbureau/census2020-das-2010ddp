import programs.schema.schemas.Schema_SF1 as myschema
import programs.schema.table_building.tablebuilder as tablebuilder
import numpy as np
import pandas as pd
import das_utils

def getTableDict():
    tabledict = {
        ################################################
        # SF1 Proposed Tables for 2020 (Person-focused)
        ################################################
        
        # Table P1 - Total Population
        # Universe: Total Population
        "P1": [
            "total"
        ],
        
        # Table P3 - Race
        # Universe: Total Population
        "P3": [
            "total",
            "majorRaces"
        ],
        
        # Table P4 - Hispanic or Latino
        # Universe: Total Population
        "P4": [
            "total",
            "hispanic"
        ],
        
        # Table P5 - Hispanic or Latino by Race
        # Universe: Total Population
        "P5": [
            "total",
            "hispanic",
            "majorRaces",
            "hispanic * majorRaces"
        ],
        
        # Table P6 - Race (Total Races Tallied)
        # Universe: Total Races Tallied ???
        # What does the total races tallied query mean??? Do we use "total" or some
        # recode that totals the racecomb query answers?
        "P6": [
            #"racecombTotal",
            #"total",
            "racecomb"
        ],
        
        # Table P7 - Hispanic or Latino by Race (Total Races Tallied)
        # Universe: Total Races Tallied ???
        # What does the total races tallied query mean??? Do we use "total" or some
        # recode that totals the racecomb query answers?
        "P7": [
            #"racecombTotal",
            #"total",
            #"hispanic * racecombTotal",
            #"hispanic",
            "racecomb",
            "hispanic * racecomb"
        ],
        
        # Table P12 - Sex by Age
        # Universe: Total Population
        "P12": [
            "total",
            "sex",
            "agecat",
            "sex * agecat"
        ],
        
        # Table P14 - Sex by Age for the Population Under 20 Years
        # Universe: Population Under 20 Years
        
        "P14": [
            "under20yearsTotal",
            "under20yearsTotal * sex",
            "under20years",
            "under20years * sex"
        ],
        
        # Table P29 - Household Type by Relationship
        # Universe: Total Population
        # Ignoring Living alone / Not living alone variable
        "P29": [
            "total",
            "hhgq",
            "householder * sex",
            "relgq"
        ],
        
        # Table P32 - Household Type By Relationship By Age for the Population Under 18 Years
        # Universe: Population under 18 years
        "P32": [
            "under18yearsTotal",
            "under18yearsTotal * hhgq",
            "under18yearsTotal * rel32",
            "under18years",
            "under18years * hhgq",
            "under18years * rel32"
        ],
        
        # Table P34 - Household Type by Relationship for the Population 65+
        # Universe: Population 65 years and over
        "P34": [
            "over64yearsTotal",
            "over64yearsTotal * hhgq",
            "over64yearsTotal * rel34",
            "over64yearsTotal * householder * sex",
        ],
        
        # Table P43 - GQ Population by Sex by Age by Major GQ Type
        # Universe: Population in GQs
        "P43": [
            "gqTotal",
            "gqTotal * sex",
            "gqTotal * agecat43",
            "gqTotal * sex * agecat43",
            "institutionalized",
            "institutionalized * sex",
            "institutionalized * agecat43",
            "institutionalized * sex * agecat43",
            "majorGQs",
            "majorGQs * sex",
            "majorGQs * agecat43",
            "majorGQs * sex * agecat43"
        ],
        
        
        # Tables P12A-I
        # Combines individual P12A-I tables
        "P12A-I": [
            # P12A-G
            "majorRaces",
            "majorRaces * sex",
            "majorRaces * agecat",
            "majorRaces * sex * agecat",
            
            # P12H
            "hispanic",
            "hispanic * sex",
            "hispanic * agecat",
            "hispanic * sex * agecat",
            
            # P12I
            "whiteAlone * hispanic",
            "whiteAlone * hispanic * sex",
            "whiteAlone * hispanic * agecat",
            "whiteAlone * hispanic * sex * agecat"
        ],
        
        # ***Note - Skipping the "Foster child" and "Living alone" queries for Tables P29A-I***
        # Tables P29A-I
        # Combines individual P29A-I tables
        "P29A-I": [
            # P29A-G
            "majorRaces",
            "majorRaces * hhgq",
            "majorRaces * relgq",
            "majorRaces * householder * sex",
            
            # P29H
            "hispanic",
            "hispanic * hhgq",
            "hispanic * relgq",
            "hispanic * householder * sex",
            
            # P29I
            "whiteAlone * hispanic",
            "whiteAlone * hispanic * hhgq",
            "whiteAlone * hispanic * relgq",
            "whiteAlone * hispanic * householder * sex",
        ],
        
        # Tables P34A-I
        # Combines individual P34A-I tables
        "P34A-I": [
            # P34A-G
            "majorRaces * over64yearsTotal",
            "majorRaces * over64yearsTotal * hhgq",
            "majorRaces * over64yearsTotal * rel34",
            "majorRaces * over64yearsTotal * householder * sex",
            
            # P34H
            "hispanic * over64yearsTotal",
            "hispanic * over64yearsTotal * hhgq",
            "hispanic * over64yearsTotal * rel34",
            "hispanic * over64yearsTotal * householder * sex",
            
            # P34I
            "whiteAlone * hispanic * over64yearsTotal",
            "whiteAlone * hispanic * over64yearsTotal * hhgq",
            "whiteAlone * hispanic * over64yearsTotal * rel34",
            "whiteAlone * hispanic * over64yearsTotal * householder * sex"
        ],
        
        # PCO1 - Group Quarters Population by Sex by Age
        # Universe: Population in group quarters
        "PCO1": [
            "gqTotal",
            "gqTotal * sex",
            "gqTotal * agecatPCO1",
            "gqTotal * sex * agecatPCO1"
        ],
        
        # PCO2 - Group Quarters Population in Institutional Facilities by Sex by Age
        # Universe: Institutionalized Population
        "PCO2": [
            "instTotal",
            "instTotal * sex",
            "instTotal * agecatPCO1",
            "instTotal * sex * agecatPCO1"
        ],
        
        # PCO3 - GQ Pop in Correctional Facilities for Adults by Sex by Age
        # Universe: Pop in correctional facilities for adults
        "PCO3": [
            "gqCorrectionalTotal",
            "gqCorrectionalTotal * sex",
            "gqCorrectionalTotal * agecatPCO1",
            "gqCorrectionalTotal * sex * agecatPCO1"
        ],
        
        # PCO4 - GQ Pop in Juvenile Facilities by Sex by Age
        # Universe: Pop in juvenile facilities
        "PCO4": [
            "gqJuvenileTotal",
            "gqJuvenileTotal * sex",
            "gqJuvenileTotal * agecatPCO1",
            "gqJuvenileTotal * sex * agecatPCO1"
        ],
        
        # PCO5 - GQ Pop in Nursing Facilities / Skilled-Nursing Facilities by Sex by Age
        # Universe: Pop in nursing facilities/skilled-nursing facilities
        "PCO5": [
            "gqNursingTotal",
            "gqNursingTotal * sex",
            "gqNursingTotal * agecatPCO5",
            "gqNursingTotal * sex * agecatPCO5"
        ],
        
        # PCO6 - GQ Pop in Other Institutional Facilities by Sex by Age
        # Universe: Pop in other institutional facilities
        "PCO6": [
            "gqOtherInstTotal",
            "gqOtherInstTotal * sex",
            "gqOtherInstTotal * agecatPCO1",
            "gqOtherInstTotal * sex * agecatPCO1"
        ],
        
        # PCO7 - GQ Pop in Noninstitutional Facilities by Sex by Age
        # Universe: Pop in noninstitutional facilities
        "PCO7": [
            "noninstTotal",
            "noninstTotal * sex",
            "noninstTotal * agecatPCO7",
            "noninstTotal * sex * agecatPCO7"
        ],
        
        # PCO8 - GQ Pop in College/University Student Housing by Sex by Age
        # Universe: Pop in college/university student housing
        "PCO8": [
            "gqCollegeTotal",
            "gqCollegeTotal * sex",
            "gqCollegeTotal * agecatPCO8",
            "gqCollegeTotal * sex * agecatPCO8"
        ],
        
        # PCO9 - GQ Pop in Military Quarters by Sex by Age
        # Universe: Pop in military quarters
        "PCO9": [
            "gqMilitaryTotal",
            "gqMilitaryTotal * sex",
            "gqMilitaryTotal * agecatPCO8",
            "gqMilitaryTotal * sex * agecatPCO8"
        ],
        
        # PCO10 - GQ Pop in Other Noninstitutional Facilities by Sex by Age
        # Universe: Pop in other noninstitutional facilities
        "PCO10": [
            "gqOtherNoninstTotal",
            "gqOtherNoninstTotal * sex",
            "gqOtherNoninstTotal * agecatPCO7",
            "gqOtherNoninstTotal * sex * agecatPCO7"
        ],
        
        # Tables PCO43A-I
        # Combines individual PCO43A-I tables
        "PCO43A-I": [
            # PCO43A-G
            "majorRaces * gqTotal",
            "majorRaces * gqTotal * sex",
            "majorRaces * gqTotal * agecat43",
            "majorRaces * gqTotal * sex * agecat43",
            "majorRaces * institutionalized",
            "majorRaces * institutionalized * sex",
            "majorRaces * institutionalized * agecat43",
            "majorRaces * institutionalized * sex * agecat43",
            "majorRaces * majorGQs",
            "majorRaces * majorGQs * sex",
            "majorRaces * majorGQs * agecat43",
            "majorRaces * majorGQs * sex * agecat43",
            
            # PCO43H
            "hispanic * gqTotal",
            "hispanic * gqTotal * sex",
            "hispanic * gqTotal * agecat43",
            "hispanic * gqTotal * sex * agecat43",
            "hispanic * institutionalized",
            "hispanic * institutionalized * sex",
            "hispanic * institutionalized * agecat43",
            "hispanic * institutionalized * sex * agecat43",
            "hispanic * majorGQs",
            "hispanic * majorGQs * sex",
            "hispanic * majorGQs * agecat43",
            "hispanic * majorGQs * sex * agecat43",
            
            # PCO43I
            "whiteAlone * hispanic * gqTotal",
            "whiteAlone * hispanic * gqTotal * sex",
            "whiteAlone * hispanic * gqTotal * agecat43",
            "whiteAlone * hispanic * gqTotal * sex * agecat43",
            "whiteAlone * hispanic * institutionalized",
            "whiteAlone * hispanic * institutionalized * sex",
            "whiteAlone * hispanic * institutionalized * agecat43",
            "whiteAlone * hispanic * institutionalized * sex * agecat43",
            "whiteAlone * hispanic * majorGQs",
            "whiteAlone * hispanic * majorGQs * sex",
            "whiteAlone * hispanic * majorGQs * agecat43",
            "whiteAlone * hispanic * majorGQs * sex * agecat43"
        ],
        
        # PCT12 - Sex by Age
        # Universe: Total Population
        "PCT12": [
            "total",
            "sex",
            "agecatPCT12",
            "sex * agecatPCT12"
        ],
        
        # PCT21 - GQ Pop by Sex by Age by GQ Type
        # Universe: Pop in group quarters
        "PCT21": [
            "gqTotal",
            "gqTotal * sex",
            "gqTotal * agecat43",
            "gqTotal * sex * agecat43",
            "institutionalized",
            "institutionalized * sex",
            "institutionalized * agecat43",
            "institutionalized * sex * agecat43",
            "majorGQs",
            "majorGQs * sex",
            "majorGQs * agecat43",
            "majorGQs * sex * agecat43",
            "minorGQs",
            "minorGQs * sex",
            "minorGQs * agecat43",
            "minorGQs * sex * agecat43"
        ],
        
        # PCT22 - GQ Pop by Sex by Major GQ Type for Pop 18 Years and Over
        # Universe: Pop 18 years and over in group quarters
        "PCT22": [
            "over17yearsTotal * gqTotal",
            "over17yearsTotal * gqTotal * sex",
            "over17yearsTotal * institutionalized",
            "over17yearsTotal * institutionalized * sex",
            "over17yearsTotal * majorGQs",
            "over17yearsTotal * majorGQs * sex"
        ]
    }
    
    return tabledict


def getTableBuilder():
    schema = myschema.buildSchema()
    tabledict = getTableDict()
    builder = tablebuilder.TableBuilder(schema, tabledict)
    return builder


###############################################################################
# Notes about SF1 Tables
# Indented tables are those that were skipped
###############################################################################
"""
P1 - Total Population

    P2 - Urban and Rural - ***Skipped***

P3 - Race

P4 - Hispanic or Latino

P5 - Hispanic or Latino by Race

P6 - Race (Total Races Tallied)

P7 - Hispanic or Latino by Race (Total Races Tallied)

    P8-11 - ***Skipped b/c Deleted for 2020***

P12 - Sex by Age

    P13 - Median Age by Sex - ***Skipped***

P14 - Sex by Age for the Population Under 20 Years

    P15-28 - ***Deleted for 2020 OR Skipped b/c of Universe: Households***

P29 - Household Type by Relationship

    P30 - ***Skipped b/c of Universe: Pop in Households*** ??
    
    P31 - ***Skipped b/c of Household/Relationships*** ??

P32 - Household Type By Relationship By Age for the Population Under 18 Years

    P33 - ***Skipped b/c Deleted for 2020***

P34 - Household Type by Relationship for the Population 65+

    P35 - ***Skipped b/c Deleted for 2020***

    P36-42 - ***Skipped b/c of Universe: Families OR Deleted for 2020***

P43 - GQ Population by Sex by Age by Major GQ Type

    P44-51 - ***Deleted for 2020***

P12A - Sex by Age (White alone)
P12B - Sex by Age (Black or African American alone)
P12C - Sex by Age (American Indian and Alaska Native alone)
P12D - Sex by Age (Asian alone)
P12E - Sex by Age (Native Hawaiian and Other Pacific Islander alone)
P12F - Sex by Age (Some Other Race alone)
P12G - Sex by Age (Two or more races)
P12H - Sex by Age (Hispanic or Latino)
P12I - Sex by Age (White alone, not Hispanic or Latino)

    P13A-I - ***Skipped b/c Median Age By Sex tables***
    
    P16A-28I - ***Skipped b/c Universe: Household***
    
***Note - Skipping the "Foster child" and "Living alone" queries for Tables P29A-I***
P29A - Household Type By Relationship (White alone)
P29B - Household Type By Relationship (Black or African American alone)
P29C - Household Type By Relationship (American Indian and Alaska Native alone)
P29D - Household Type By Relationship (Asian alone)
P29E - Household Type By Relationship (Native Hawaiian and Pacific Islander alone)
P29F - Household Type By Relationship (Some Other Race alone)
P29G - Household Type By Relationship (Two or more races)
P29H - Household Type By Relationship (Hispanic or Latino)
P29I - Household Type By Relationship (White alone, Not Hispanic or Latino)
    
    P31A-I - ***Skipping; Household-like queries***

P34A - Household Type by Relationship for the Population 65 Years and Over (White alone)
P34B - Household Type by Relationship for the Population 65 Years and Over (Black or African American alone)
P34C - Household Type by Relationship for the Population 65 Years and Over (American Indian and Alaska Native alone)
P34D - Household Type by Relationship for the Population 65 Years and Over (Asian alone)
P34E - Household Type by Relationship for the Population 65 Years and Over (Native Hawaiian and Other Pacific Islander alone)
P34F - Household Type by Relationship for the Population 65 Years and Over (Some Other Race alone)
P34G - Household Type by Relationship for the Population 65 Years and Over (Two or more races)
P34H - Household Type by Relationship for the Population 65 Years and Over (Hispanic or Latino)
P34I - Household Type by Relationship for the Population 65 Years and Over (White alone, not Hispanic or Latino)
    
    P35A-I - ***Skipping because Deleted for 2020***
    
    36A-39I - ***Skipping b/c of Universe: Families OR Deleted for 2020***

PCO1 - Group Quarters Population by Sex by Age
PCO2 - Group Quarters Population in Institutional Facilities by Sex by Age
PCO3 - GQ Pop in Correctional Facilities for Adults by Sex by Age
PCO4 - GQ Pop in Juvenile Facilities by Sex by Age
PCO5 - GQ Pop in Nursing Facilities / Skilled-Nursing Facilities by Sex by Age
PCO6 - GQ Pop in Other Institutional Facilities by Sex by Age
PCO7 - GQ Pop in Noninstitutional Facilities by Sex by Age
PCO8 - GQ Pop in College/University Student Housing by Sex by Age
PCO9 - GQ Pop in Military Quarters by Sex by Age
PCO10 - GQ Pop in Other Noninstitutional Facilities by Sex by Age

PCO43A - GQ Pop by Sex by Age by Major GQ Type (White alone)
PCO43B - GQ Pop by Sex by Age by Major GQ Type (Black or African American alone)
PCO43C - GQ Pop by Sex by Age by Major GQ Type (American Indian or Alaska Native alone)
PCO43D - GQ Pop by Sex by Age by Major GQ Type (Asian alone)
PCO43E - GQ Pop by Sex by Age by Major GQ Type (Native Hawaiian or Other Pacific Islander alone)
PCO43F - GQ Pop by Sex by Age by Major GQ Type (Some Other Race alone)
PCO43G - GQ Pop by Sex by Age by Major GQ Type (Two or More Races Alone)
PCO43H - GQ Pop by Sex by Age by Major GQ Type (Hispanic or Latino)
PCO43I - GQ Pop by Sex by Age by Major GQ Type (White Alone, Not Hispanic or Latino)
    
    PCT #### tables - Skipping because race details are finer than what exists in the histogram

PCT12 - Sex by Age
    
    PCT13-20 - Skipping because either Household-related or Deleted for 2020

PCT21 - GQ Pop by Sex by Age by GQ Type

PCT22 - GQ Pop by Sex by Major GQ Type for Pop 18 Years and Over
    
    PCT23-PCT12A-O - Deleted for 2020
    
    PCT13A-PCT22I - Skipped because they have to do with Households/Housing units OR because they were Deleted for 2020
    
    H1-H22 - Skipped because they have to do with Households/Housing units OR because they were Deleted for 2020
    
    H11A-H17I - Skipped because they have to do with Households/Housing units
    
    HCT1-HCT4 - Skipped because they have to do with Households/Housing units
"""



###############################################################################
# Individual A-I tables that have been coalesced above
###############################################################################

####################
# P12A-I Tables
####################
## Table P12A - Sex by Age (White alone)
## Universe: People who are White alone
#"P12A": [
    #"whiteAlone",
    #"whiteAlone * sex",
    #"whiteAlone * agecat",
    #"whiteAlone * sex * agecat",
#],

## Table P12B - Sex by Age (Black or African American alone)
## Universe: People who are Black or African American alone
#"P12B": [
    #"blackAlone",
    #"blackAlone * sex",
    #"blackAlone * agecat",
    #"blackAlone * sex * agecat"
#],

## Table P12C - Sex by Age (American Indian and Alaska Native alone)
## Universe: People who are American Indian and Alaska Native alone
#"P12C": [
    #"aianAlone",
    #"aianAlone * sex",
    #"aianAlone * agecat",
    #"aianAlone * sex * agecat"
#],

## Table P12D - Sex by Age (Asian alone)
## Universe: People who are Asian alone
#"P12D": [
    #"asianAlone",
    #"asianAlone * sex",
    #"asianAlone * agecat",
    #"asianAlone * sex * agecat"
#],

## Table P12E - Sex by Age (Native Hawaiian and Other Pacific Islander alone)
## Universe: People who are Native Hawaiian and Other Pacific Islander alone)
#"P12E": [
    #"nhopiAlone",
    #"nhopiAlone * sex",
    #"nhopiAlone * agecat",
    #"nhopiAlone * sex * agecat"
#],

## Table P12F - Sex by Age (Some Other Race alone)
## Universe: People who are Some Other Race alone
#"P12F": [
    #"sorAlone",
    #"sorAlone * sex",
    #"sorAlone * agecat",
    #"sorAlone * sex * agecat"
#],

## Table P12G - Sex by Age (Two or more races)
## Universe: People who are two or more races
#"P12G": [
    #"tomr",
    #"tomr * sex",
    #"tomr * agecat",
    #"tomr * sex * agecat"
#],

## Table P12H - Sex by Age (Hispanic or Latino)
## Universe: People who are Hispanic or Latino
#"P12H": [
    #"hispTotal",
    #"hispTotal * sex",
    #"hispTotal * agecat",
    #"hispTotal * sex * agecat"
#],

## Table P12I - Sex by Age (White alone, not Hispanic or Latino)
#"P12I": [
    #"whiteAlone * notHispTotal",
    #"whiteAlone * notHispTotal * sex",
    #"whiteAlone * notHispTotal * agecat",
    #"whiteAlone * notHispTotal * sex * agecat"
#],



####################
# P29A-I Tables
####################
## Table P29A - Household Type By Relationship (White alone)
## Universe: People who are White alone
#"P29A": [
    #"whiteAlone",
    #"whiteAlone * hhgq",
    #"whiteAlone * relgq",
    #"whiteAlone * householder * sex"
#],

## Table P29B - Household Type By Relationship (Black or African American alone)
## Universe: People who are Black or African American alone
#"P29B": [
    #"blackAlone",
    #"blackAlone * hhgq",
    #"blackAlone * relgq",
    #"blackAlone * householder * sex"
#],

## Table P29C - Household Type By Relationship (American Indian and Alaska Native alone)
## Universe: People who are American Indian and Alaska Native alone
#"P29C": [
    #"aianAlone",
    #"aianAlone * hhgq",
    #"aianAlone * relgq",
    #"aianAlone * householder * sex"
#],

## Table P29D - Household Type By Relationship (Asian alone)
## Universe: People who are Asian alone
#"P29D": [
    #"asianAlone",
    #"asianAlone * hhgq",
    #"asianAlone * relgq",
    #"asianAlone * householder * sex"
#],

## Table P29E - Household Type By Relationship (Native Hawaiian and Pacific Islander alone)
## Universe: People who are Native Hawaiian and Pacific Islander alone
#"P29E": [
    #"nhopiAlone",
    #"nhopiAlone * hhgq",
    #"nhopiAlone * relgq",
    #"nhopiAlone * householder * sex"
#],

## Table P29F - Household Type By Relationship (Some Other Race alone)
## Universe: People who are Some Other Race alone
#"P29F": [
    #"sorAlone",
    #"sorAlone * hhgq",
    #"sorAlone * relgq",
    #"sorAlone * householder * sex"
#],

## Table P29G - Household Type By Relationship (Two or more races)
## Universe: People who are Two or more races
#"P29G": [
    #"tomr",
    #"tomr * hhgq",
    #"tomr * relgq",
    #"tomr * householder * sex"
#],

## Table P29H - Household Type By Relationship (Hispanic or Latino)
## Universe: People who are Hispanic or Latino
#"P29H": [
    #"hispTotal",
    #"hispTotal * hhgq",
    #"hispTotal * relgq",
    #"hispTotal * householder * sex"
#],

## Table P29I - Household Type By Relationship (White alone, Not Hispanic or Latino)
## Universe: People who are White alone, not Hispanic or Latino
#"P29I": [
    #"whiteAlone * notHispTotal",
    #"whiteAlone * notHispTotal * hhgq",
    #"whiteAlone * notHispTotal * relgq",
    #"whiteAlone * notHispTotal * householder * sex"
#],


####################
# P34A-I Tables
####################
## Table P34A - Household Type by Relationship for the Population 65 Years and Over (White alone)
## Universe: People 65 years and over who are White alone
#"P34A": [
    #"whiteAlone * over64yearsTotal",
    #"whiteAlone * over64yearsTotal * hhgq",
    #"whiteAlone * over64yearsTotal * rel34",
    #"whiteAlone * over64yearsTotal * householder * sex"
#],

## Table P34B - Household Type by Relationship for the Population 65 Years and Over (Black or African American alone)
## Universe: People 65 years and over who are Black or African American alone
#"P34B": [
    #"blackAlone * over64yearsTotal",
    #"blackAlone * over64yearsTotal * hhgq",
    #"blackAlone * over64yearsTotal * rel34",
    #"blackAlone * over64yearsTotal * householder * sex"
#],

## Table P34C - Household Type by Relationship for the Population 65 Years and Over (American Indian and Alaska Native alone)
## Universe: People 65 years and over who are American Indian and Alaska Native alone
#"P34C": [
    #"aianAlone * over64yearsTotal",
    #"aianAlone * over64yearsTotal * hhgq",
    #"aianAlone * over64yearsTotal * rel34",
    #"aianAlone * over64yearsTotal * householder * sex"
#],

## Table P34D - Household Type by Relationship for the Population 65 Years and Over (Asian alone)
## Universe: People 65 years and over who are Asian alone
#"P34D": [
    #"asianAlone * over64yearsTotal",
    #"asianAlone * over64yearsTotal * hhgq",
    #"asianAlone * over64yearsTotal * rel34",
    #"asianAlone * over64yearsTotal * householder * sex"
#],

## Table P34E - Household Type by Relationship for the Population 65 Years and Over (Native Hawaiian and Other Pacific Islander alone)
## Universe: People 65 years and over who are Native Hawaiian and Other Pacific Islander alone
#"P34E": [
    #"nhopiAlone * over64yearsTotal",
    #"nhopiAlone * over64yearsTotal * hhgq",
    #"nhopiAlone * over64yearsTotal * rel34",
    #"nhopiAlone * over64yearsTotal * householder * sex"
#],

## Table P34F - Household Type by Relationship for the Population 65 Years and Over (Some Other Race alone)
## Universe: People 65 years and over who are Some Other Race alone
#"P34F": [
    #"sorAlone * over64yearsTotal",
    #"sorAlone * over64yearsTotal * hhgq",
    #"sorAlone * over64yearsTotal * rel34",
    #"sorAlone * over64yearsTotal * householder * sex"
#],

## Table P34G - Household Type by Relationship for the Population 65 Years and Over (Two or more races)
## Universe: People 65 years and over who are Two or more races
#"P34G": [
    #"tomr * over64yearsTotal",
    #"tomr * over64yearsTotal * hhgq",
    #"tomr * over64yearsTotal * rel34",
    #"tomr * over64yearsTotal * householder * sex"
#],

## Table P34H - Household Type by Relationship for the Population 65 Years and Over (Hispanic or Latino)
## Universe: People 65 years and over who are Hispanic or Latino
#"P34H": [
    #"hispTotal * over64yearsTotal",
    #"hispTotal * over64yearsTotal * hhgq",
    #"hispTotal * over64yearsTotal * rel34",
    #"hispTotal * over64yearsTotal * householder * sex"
#],

## Table P34I - Household Type by Relationship for the Population 65 Years and Over (White alone, not Hispanic or Latino)
## Universe: People 65 years and over who are White alone, not Hispanic or Latino
#"P34I": [
    #"whiteAlone * notHispTotal * over64yearsTotal",
    #"whiteAlone * notHispTotal * over64yearsTotal * hhgq",
    #"whiteAlone * notHispTotal * over64yearsTotal * rel34",
    #"whiteAlone * notHispTotal * over64yearsTotal * householder * sex"
#],



####################
# PCO43 A-I Tables
####################
## PCO43A - GQ Pop by Sex by Age by Major GQ Type (White alone)
## Universe: Pop in group quarters
#"PCO43A": [
    #"whiteAlone * gqTotal",
    #"whiteAlone * gqTotal * sex",
    #"whiteAlone * gqTotal * agecat43",
    #"whiteAlone * gqTotal * sex * agecat43",
    #"whiteAlone * institutionalized",
    #"whiteAlone * institutionalized * sex",
    #"whiteAlone * institutionalized * agecat43",
    #"whiteAlone * institutionalized * sex * agecat43",
    #"whiteAlone * majorGQs",
    #"whiteAlone * majorGQs * sex",
    #"whiteAlone * majorGQs * agecat43",
    #"whiteAlone * majorGQs * sex * agecat43"
#],

## PCO43B - GQ Pop by Sex by Age by Major GQ Type (Black or African American alone)
## Universe: Pop in group quarters
#"PCO43B": [
    #"blackAlone * gqTotal",
    #"blackAlone * gqTotal * sex",
    #"blackAlone * gqTotal * agecat43",
    #"blackAlone * gqTotal * sex * agecat43",
    #"blackAlone * institutionalized",
    #"blackAlone * institutionalized * sex",
    #"blackAlone * institutionalized * agecat43",
    #"blackAlone * institutionalized * sex * agecat43",
    #"blackAlone * majorGQs",
    #"blackAlone * majorGQs * sex",
    #"blackAlone * majorGQs * agecat43",
    #"blackAlone * majorGQs * sex * agecat43"
#],

## PCO43C - GQ Pop by Sex by Age by Major GQ Type (American Indian or Alaska Native alone)
## Universe: Pop in group quarters
#"PCO43C": [
    #"aianAlone * gqTotal",
    #"aianAlone * gqTotal * sex",
    #"aianAlone * gqTotal * agecat43",
    #"aianAlone * gqTotal * sex * agecat43",
    #"aianAlone * institutionalized",
    #"aianAlone * institutionalized * sex",
    #"aianAlone * institutionalized * agecat43",
    #"aianAlone * institutionalized * sex * agecat43",
    #"aianAlone * majorGQs",
    #"aianAlone * majorGQs * sex",
    #"aianAlone * majorGQs * agecat43",
    #"aianAlone * majorGQs * sex * agecat43"
#],

## PCO43D - GQ Pop by Sex by Age by Major GQ Type (Asian alone)
## Universe: Pop in group quarters
#"PCO43D": [
    #"asianAlone * gqTotal",
    #"asianAlone * gqTotal * sex",
    #"asianAlone * gqTotal * agecat43",
    #"asianAlone * gqTotal * sex * agecat43",
    #"asianAlone * institutionalized",
    #"asianAlone * institutionalized * sex",
    #"asianAlone * institutionalized * agecat43",
    #"asianAlone * institutionalized * sex * agecat43",
    #"asianAlone * majorGQs",
    #"asianAlone * majorGQs * sex",
    #"asianAlone * majorGQs * agecat43",
    #"asianAlone * majorGQs * sex * agecat43"
#],

## PCO43E - GQ Pop by Sex by Age by Major GQ Type (Native Hawaiian or Other Pacific Islander alone)
## Universe: Pop in group quarters
#"PCO43E": [
    #"nhopiAlone * gqTotal",
    #"nhopiAlone * gqTotal * sex",
    #"nhopiAlone * gqTotal * agecat43",
    #"nhopiAlone * gqTotal * sex * agecat43",
    #"nhopiAlone * institutionalized",
    #"nhopiAlone * institutionalized * sex",
    #"nhopiAlone * institutionalized * agecat43",
    #"nhopiAlone * institutionalized * sex * agecat43",
    #"nhopiAlone * majorGQs",
    #"nhopiAlone * majorGQs * sex",
    #"nhopiAlone * majorGQs * agecat43",
    #"nhopiAlone * majorGQs * sex * agecat43"
#],

## PCO43F - GQ Pop by Sex by Age by Major GQ Type (Some Other Race alone)
## Universe: Pop in group quarters
#"PCO43F": [
    #"sorAlone * gqTotal",
    #"sorAlone * gqTotal * sex",
    #"sorAlone * gqTotal * agecat43",
    #"sorAlone * gqTotal * sex * agecat43",
    #"sorAlone * institutionalized",
    #"sorAlone * institutionalized * sex",
    #"sorAlone * institutionalized * agecat43",
    #"sorAlone * institutionalized * sex * agecat43",
    #"sorAlone * majorGQs",
    #"sorAlone * majorGQs * sex",
    #"sorAlone * majorGQs * agecat43",
    #"sorAlone * majorGQs * sex * agecat43"
#],

## PCO43G - GQ Pop by Sex by Age by Major GQ Type (Two or More Races Alone)
## Universe: Pop in group quarters
#"PCO43G": [
    #"tomr * gqTotal",
    #"tomr * gqTotal * sex",
    #"tomr * gqTotal * agecat43",
    #"tomr * gqTotal * sex * agecat43",
    #"tomr * institutionalized",
    #"tomr * institutionalized * sex",
    #"tomr * institutionalized * agecat43",
    #"tomr * institutionalized * sex * agecat43",
    #"tomr * majorGQs",
    #"tomr * majorGQs * sex",
    #"tomr * majorGQs * agecat43",
    #"tomr * majorGQs * sex * agecat43"
#],

## PCO43H - GQ Pop by Sex by Age by Major GQ Type (Hispanic or Latino)
## Universe: Pop in group quarters
#"PCO43H": [
    #"hispTotal * gqTotal",
    #"hispTotal * gqTotal * sex",
    #"hispTotal * gqTotal * agecat43",
    #"hispTotal * gqTotal * sex * agecat43",
    #"hispTotal * institutionalized",
    #"hispTotal * institutionalized * sex",
    #"hispTotal * institutionalized * agecat43",
    #"hispTotal * institutionalized * sex * agecat43",
    #"hispTotal * majorGQs",
    #"hispTotal * majorGQs * sex",
    #"hispTotal * majorGQs * agecat43",
    #"hispTotal * majorGQs * sex * agecat43"
#],

## PCO43I - GQ Pop by Sex by Age by Major GQ Type (White Alone, Not Hispanic or Latino)
## Universe: Pop in group quarters
##### The [complement/partition of each race * not hispanic] of the universe might be useful ####
#"PCO43I": [
    #"whiteAlone * notHispTotal * gqTotal",
    #"whiteAlone * notHispTotal * gqTotal * sex",
    #"whiteAlone * notHispTotal * gqTotal * agecat43",
    #"whiteAlone * notHispTotal * gqTotal * sex * agecat43",
    #"whiteAlone * notHispTotal * institutionalized",
    #"whiteAlone * notHispTotal * institutionalized * sex",
    #"whiteAlone * notHispTotal * institutionalized * agecat43",
    #"whiteAlone * notHispTotal * institutionalized * sex * agecat43",
    #"whiteAlone * notHispTotal * majorGQs",
    #"whiteAlone * notHispTotal * majorGQs * sex",
    #"whiteAlone * notHispTotal * majorGQs * agecat43",
    #"whiteAlone * notHispTotal * majorGQs * sex * agecat43"
#],
