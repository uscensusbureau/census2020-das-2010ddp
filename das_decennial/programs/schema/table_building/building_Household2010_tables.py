import programs.schema.schemas.Schema_Household2010 as myschema
import programs.schema.table_building.tablebuilder as tablebuilder
import numpy as np
import pandas as pd
import das_utils

def getTableDict():
    tabledict = {
        ################################################
        # Household2010 Proposed Tables for 2020
        ################################################
        
        # Table P15 - Hispanic or Latino Origin of Householder by Race of Householder
        # Universe: Households
        "P15": [
            "total",
            
            "hisp",
            "hisp * hhrace"
        ],
        
        # Table P16 - Population in Households by Age
        # Universe: Population in households
        # Cannot complete using Household2010 Schema since the hhage variable doesn't support the needed queries
        
        # Table P18 - Household Type
        # Universe: Households
        "P18": [
            "total",
            
            "family",
            "nonfamily",
            "marriedfamily",
            "otherfamily",
            "otherfamily * hhsex",
            "alone",
            "notalone",
        ],
        
        # Table P19 - Household Size by Household Type by Presence of Own Children
        # Universe: Households
        "P19": [
            "total",
            
            "size1",
            "size2plus",
            "hhsex * size1",
            "family",
            "nonfamily",
            "marriedfamily",
            "married_with_children_indicator",
            "otherfamily",
            "hhsex * otherfamily",
            "hhsex * other_with_children_indicator",
            "nonfamily * hhsex",
            "notalone",
            "hhsex * notalone",
        ],
        
        # Table 20 - Households by Type and Presence of Own Children Under 18
        # Universe: Households
        "P20": [
            "total",
            
            "marriedfamily",
            "married_with_children_indicator",
            "cohabiting",
            "cohabiting_with_children_indicator",
            "hhsex * no_spouse_or_partner",
            "hhsex * no_spouse_or_partner_levels"
        ],
        
        # Table P22 - Household Type by Age of Householder
        # Universe: Households
        "P22": [
            "total",
            
            "family",
            "hhage * family",
            "nonfamily",
            "hhage * nonfamily"
        ],
        
        # Table P23 - Households by Presence of People 60 Years and Over by Household Type
        # Universe: Households
        "P23": [
            "total",
            
            "presence60",
            "family * presence60",
            "marriedfamily * presence60",
            "otherfamily * presence60",
            "hhsex * otherfamily * presence60",
            "nonfamily * presence60"
        ],
        
        # Table P24 - Households by Presence of People 60 Years and Over, Household Size, and Household Type
        # Universe: Households
        "P24": [
            "total",
            
            "presence60",
            "size1 * presence60",
            "size2plus * presence60",
            "notalone * presence60",
            "family * presence60"
        ],
        
        # Table P25 - Households by Presence of People 65 Years and Over, Household Size, and Household Type
        # Universe: Households
        "P25": [
            "total",
            
            "presence65",
            "presence65 * size1",
            "presence65 * size2plus",
            "presence65 * notalone",
            "presence65 * family",
        ],
        
        # Table P26 - Households by Presence of People 75 Years and Over, Household Size, and Household Type
        # Universe: Households
        "P26": [
            "total",
            
            "presence75",
            "presence75 * size1",
            "presence75 * size2plus",
            "presence75 * notalone",
            "presence75 * family",
        ],
        
        # Table P28 - Household Type by Household Size
        "P28": [
            "total",
            
            "family",
            "nonfamily",
            "family * sizex01",
            "nonfamily * sizex0",
        ],
        
        # Table P30 - Household Type for the Population in Households
        # Universe: Population in Households
        #"P30": [
            #"total",
            #"family",
            #"marriedfamily",
            #"otherfamily",
            #"no_spouse_or_partner * hhsex",
            #"nonfamily",
            #"nonfamily * hhsex",
            #"nonfamily * hhsex * size1",
            #"nonfamily * hhsex * size2plus",
            #"nonfamily * size1",
            #"nonfamily * size2plus",
            #"family * size1",     # ???
            #"family * size2plus", # ???
            #"size1",
            #"size2plus",
            #"no_spouse_or_partner",
        #],
        
        # Table P38 - Family Type by Presence and Age of Own Children
        # Universe: Families 
        "P38": [
            "total",
            
            "marriedfamily",
            "married_with_children_indicator",
            "married_with_children_levels",
            "otherfamily",
            "hhsex * otherfamily",
            "hhsex * other_with_children_indicator",
            "hhsex * other_with_children_levels"
        ],
        
        # Tables P18A-G
        "P18A-G": [
            "hhrace", # to get the totals for each race
            
            "hhrace * family",
            "hhrace * nonfamily",
            "hhrace * marriedfamily",
            "hhrace * otherfamily",
            "hhrace * otherfamily * hhsex",
            "hhrace * alone",
            "hhrace * notalone"
        ],
        
        # Table P18H
        "P18H": [
            "hisp", # to get the totals for each hispanic category
            
            "hisp * family",
            "hisp * nonfamily",
            "hisp * marriedfamily",
            "hisp * otherfamily",
            "hisp * otherfamily * hhsex",
            "hisp * alone",
            "hisp * notalone"
        ],
        
        # Table P18I
        "P18I": [
            "whiteonly * hisp", # to get the totals for each of the white alone x hispanic categories
            
            "whiteonly * hisp * family",
            "whiteonly * hisp * nonfamily",
            "whiteonly * hisp * marriedfamily",
            "whiteonly * hisp * otherfamily",
            "whiteonly * hisp * otherfamily * hhsex",
            "whiteonly * hisp * alone",
            "whiteonly * hisp * notalone"
        ],
        
        # Tables P28A-G
        "P28A-G": [
            "hhrace", # to get the totals for each race
            
            "hhrace * family",
            "hhrace * family * sizex01",
            "hhrace * nonfamily",
            "hhrace * nonfamily * sizex0"
        ],
        
        # Table P28H
        "P28H": [
            "hisp", # to get the totals for each of the hispanic categories
            
            "hisp * family",
            "hisp * family * sizex01",
            "hisp * nonfamily",
            "hisp * nonfamily * sizex0"
        ],
        
        # Table P28I
        "P28I": [
            "whiteonly * hisp", # to get the totals for each of the white alone x hispanic categories
            
            "whiteonly * hisp * family",
            "whiteonly * hisp * family * sizex01",
            "whiteonly * hisp * nonfamily",
            "whiteonly * hisp * nonfamily * sizex0"
        ],
        
        # Tables P38A-G
        "P38A-G": [
            # Note: since "marriedfamily" and "otherfamily" don't fully partition the space, 
            # the "hhrace * marriedfamily" and "hhrace * otherfamily" queries may not sum up 
            # to the "hhrace" query totals
            "hhrace", # to get the totals for each race
            
            "hhrace * marriedfamily",
            "hhrace * married_with_children_indicator",
            "hhrace * married_with_children_levels",
            "hhrace * otherfamily",
            "hhrace * hhsex * otherfamily",
            "hhrace * hhsex * other_with_children_indicator",
            "hhrace * hhsex * other_with_children_levels"
        ],
        
        # Table P38H
        "P38H": [
            # Note: since "marriedfamily" and "otherfamily" don't fully partition the space, 
            # the "hisp * marriedfamily" and "hisp * otherfamily" queries may not sum up 
            # to the "hisp" query totals
            "hisp", # to get the totals for each of the hispanic categories
            
            "hisp * marriedfamily",
            "hisp * married_with_children_indicator",
            "hisp * married_with_children_levels",
            "hisp * otherfamily",
            "hisp * hhsex * otherfamily",
            "hisp * hhsex * other_with_children_indicator",
            "hisp * hhsex * other_with_children_levels"
        ],
        
        # Table P38I
        "P38I": [
            # Note: since "marriedfamily" and "otherfamily" don't fully partition the space, 
            # the "whiteonly * hisp * marriedfamily" and "whiteonly * hisp * otherfamily" queries may not sum up 
            # to the "whiteonly * hisp" query totals
            "whiteonly * hisp", # to get the totals for white alone x hispanic categories
            
            "whiteonly * hisp * marriedfamily",
            "whiteonly * hisp * married_with_children_indicator",
            "whiteonly * hisp * married_with_children_levels",
            "whiteonly * hisp * otherfamily",
            "whiteonly * hisp * hhsex * otherfamily",
            "whiteonly * hisp * hhsex * other_with_children_indicator",
            "whiteonly * hisp * hhsex * other_with_children_levels"
        ],
        
        # Table PCT14
        "PCT14": [
            "total",
            
            "multi"
        ],
        
        # Table PCT15
        "PCT15": [
            "total",
            
            "couplelevels",
            "oppositelevels",
            "samelevels",
            "hhsex * samelevels"
        ],
        
        # Table PCT18
        "PCT18": [
            "total",
            
            "hhsex * nonfamily",
            "hhsex * alone",
            "hhsex * alone * hhover65",
            "hhsex * notalone",
            "hhsex * notalone * hhover65"
        ],
        
        # Tables PCT14A-I
        "PCT14A-I": [
            # get the totals of the following variables
            "hhrace",
            "hisp",
            "whiteonly * hisp",
            
            "hhrace * multi",
            "hisp * multi",
            "whiteonly * hisp * multi"
        ],
        
        # Table H6
        "H6": [
            "total",
            
            "hhrace"
        ],
        
        # Table H7
        "H7": [
            "total",
            
            # do we need to explicitly express the marginals?
            #"hisp", 
            #"hhrace",
            
            "hisp * hhrace",
        ],
        
        # Table H13
        "H13": [
            "total",
            
            "sizex0"
        ],
        
        # Table H14
        "H14": [
            "total",
            
            "rent",
            "rent * hhrace"
        ],
        
        # Table H15
        "H15": [
            "total",
            
            "rent",
            "rent * hisp"
        ],
        
        # Table H16
        "H16": [
            "total",
            
            "rent",
            "rent * sizex0"
        ],
        
        # Table H17
        "H17": [
            "total",
            
            "rent",
            "rent * hhage"
        ],
        
        # Table H18
        "H18": [
            "total",
            
            "rent",
            "rent * family",
            "rent * marriedfamily",
            "rent * marriedfamily * hhageH18",
            "rent * otherfamily",
            "rent * otherfamily * hhsex",
            "rent * otherfamily * hhsex * hhageH18",
            "rent * nonfamily",
            "rent * nonfamily * hhsex",
            "rent * alone * hhsex * hhageH18",
            "rent * notalone * hhsex * hhageH18"
        ],
        
        # Tables H16A-I
        "H16A-I": [
            "hhrace",
            "hisp",
            "whiteonly * hisp",
            
            "hhrace * rent",
            "hhrace * rent * sizex0",
            "hisp * rent",
            "hisp * rent * sizex0",
            "whiteonly * hisp * rent",
            "whiteonly * hisp * rent * sizex0"
        ],
        
        # Tables H17A-I
        "H17A-I": [
            "hhrace",
            "hisp",
            "whiteonly * hisp",
            
            "hhrace * rent",
            "hisp * rent",
            "whiteonly * hisp * rent",
            "hhrace * rent * hhage",
            "hisp * rent * hhage",
            "whiteonly * hisp * rent * hhage"
        ],
        
        # Table HCT1
        "HCT1": [
            "total",
            
            "rent",
            "rent * hisp",
            "rent * hisp * hhrace"
        ],
        
        # Table HCT2
        #"HCT2": [
            ## needs recodes (e.g. with_children_indicator/with_children_levels)
            #"rent * with_children_indicator",
            #"rent * with_children_levels"
        #],
        
        #Table HCT4
    }
    
    return tabledict



def getTableBuilder():
    schema = myschema.buildSchema()
    tabledict = getTableDict()
    builder = tablebuilder.TableBuilder(schema, tabledict)
    return builder

