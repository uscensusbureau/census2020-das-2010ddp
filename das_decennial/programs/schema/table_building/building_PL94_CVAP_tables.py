import programs.schema.schemas.Schema_PL94_CVAP as myschema
import programs.schema.table_building.tablebuilder as tablebuilder
import numpy as np
import pandas as pd
import das_utils

def getTableDict():
    tabledict = {
        ################################################
        # PL94 Tables
        ################################################
        
        # Table P1 - Race
        # Universe: Total Population
        "P1": [
            "total",
            "numraces",
            "cenrace"
        ],
        
        # Table P2 - Hispanic or Latino by Race
        # Universe: Total Population
        "P2": [
            "total",
            "hispanic",
            "numraces",
            "cenrace",
            "hispanic * numraces",
            "hispanic * cenrace"
        ],
        
        # Table P3 - Race for the Population 18 Years and over
        # Universe: Total Population 18 years and over
        # Note: The table only requires the 'voting' (i.e. Voting Age population) subset (see the commented-out queries),
        #       however, we might also want to know about the Non-Voting Age population (i.e. the complement),
        #       so we will use 'votingage', which asks two queries, one about the nonvoting
        #       population and one about the voting population
        "P3": [
            "votingage",
            "votingage * numraces",
            "votingage * cenrace"
        ],

        
        # Table P4 - Hispanic or Latino by Race for the Population 18 Years and over
        # Universe: Total Population 18 years and over
        # Note: The table only requires the 'voting' (i.e. Voting Age population) subset (see the commented-out queries),
        #       however, we might also want to know about the Non-Voting Age population (i.e. the complement),
        #       so we will use 'votingage', which asks two queries, one about the nonvoting
        #       population and one about the voting population
        "P4": [
            "votingage",
            "votingage * hispanic",
            "votingage * numraces",
            "votingage * cenrace",
            "votingage * hispanic * numraces",
            "votingage * hispanic * cenrace"
        ],
        
        # Table P42 - Group Quarters Population by GQ Type
        # Universe: Population in Group Quarters
        # Note: For the table, only 'gqTotal' is needed, given the universe,
        #       however, the complement of the universe implies that we
        #       might also want to know about households, so we'll use
        #       'household' to query BOTH the household total and gqTotal
        "P42": [
            "household",
            "institutionalized",
            "gqlevels"
        ],
        









        # Table P1_CVAP - Race by Citizenship
        # Universe: Total Population
        "P1_CVAP": [
            "total",
            "numraces",
            "cenrace",
            
            "citizen",
            "numraces * citizen",
            "cenrace * citizen"
        ],
        
        # Table P2_CVAP - Hispanic or Latino by Race by Citizenship
        # Universe: Total Population
        "P2_CVAP": [
            "total",
            "hispanic",
            "numraces",
            "cenrace",
            "hispanic * numraces",
            "hispanic * cenrace",

            "citizen",
            "hispanic * citizen",
            "numraces * citizen",
            "cenrace * citizen",
            "hispanic * numraces * citizen",
            "hispanic * cenrace * citizen"
        ],
        
        # Table P3_CVAP - Race for the Population 18 Years and over by Citizenship
        # Universe: Total Population 18 years and over
        # Note: The table only requires the 'voting' (i.e. Voting Age population) subset (see the commented-out queries),
        #       however, we might also want to know about the Non-Voting Age population (i.e. the complement),
        #       so we will use 'votingage', which asks two queries, one about the nonvoting
        #       population and one about the voting population
        "P3_CVAP": [
            "votingage",
            "votingage * numraces",
            "votingage * cenrace",

            "votingage * citizen",
            "votingage * numraces * citizen",
            "votingage * cenrace * citizen"
        ],

        
        # Table P4_CVAP - Hispanic or Latino by Race for the Population 18 Years and over by Citizenship
        # Universe: Total Population 18 years and over
        # Note: The table only requires the 'voting' (i.e. Voting Age population) subset (see the commented-out queries),
        #       however, we might also want to know about the Non-Voting Age population (i.e. the complement),
        #       so we will use 'votingage', which asks two queries, one about the nonvoting
        #       population and one about the voting population
        "P4_CVAP": [
            "votingage",
            "votingage * hispanic",
            "votingage * numraces",
            "votingage * cenrace",
            "votingage * hispanic * numraces",
            "votingage * hispanic * cenrace",
            
            "votingage * citizen",
            "votingage * hispanic * citizen",
            "votingage * numraces * citizen",
            "votingage * cenrace * citizen",
            "votingage * hispanic * numraces * citizen",
            "votingage * hispanic * cenrace * citizen"
        ],
    }
    
    return tabledict


def getTableBuilder():
    schema = myschema.buildSchema()
    tabledict = getTableDict()
    builder = tablebuilder.TableBuilder(schema, tabledict)
    return builder
