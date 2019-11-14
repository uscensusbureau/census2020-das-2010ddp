
import programs.cenrace as cenrace
import programs.schema.schema as sk
import numpy as np
import das_utils
from programs.schema.schemas.Schema_SF1 import SchemaSF1Factory, REL, SEX, AGE, HISP, CENRACE
from constants import DHCP
#from constants import *

HHGQ = "hhgq"
CITIZEN = "citizen"

# Stub for backward compatibility
def buildSchema(path=None):
    return SchemaDHCPHHGQFactory(
        name="DHCP_HHGQ", 
        dimnames=[HHGQ, SEX, AGE, HISP, CENRACE, CITIZEN],
        shape=(8, 2, 116, 2, 63, 2),
        path=path
    ).getSchema()


class SchemaDHCPHHGQFactory(SchemaSF1Factory):

    def addRecodes(self):
        ###############################################################################
        # Recodes for SF1
        ###############################################################################
        recodes_to_add = [
            self.instlevels(),
            self.votinglevels(),
            self.householdTotal(),  
            self.gqlevels(),
            self.agecat43(),
            self.institutionalized(),
            self.numraces(),
            self.votingage(),
            self.majorRaces(),
            self.majorGQs(),
            self.racecomb(),
            self.agecat(),
            self.under20years(),
            self.under20yearsTotal(),
            self.under18years(),
            self.under18yearsTotal(),
            self.over64yearsTotal(),
            self.agegroup4(),
            self.agegroup16(),
            self.agegroup64(),
            self.household(),
            self.gqTotal(),
            self.whiteAlone(),
            self.blackAlone(),
            self.aianAlone(),
            self.asianAlone(),
            self.nhopiAlone(),
            self.sorAlone(),
            self.tomr(),
            self.agecatPCO1(),
            self.agecatPCO3(),
            self.agecatPCO4(),
            self.instTotal(),
            self.gqCorrectionalTotal(),
            self.gqJuvenileTotal(),
            self.agecatPCO5(),
            self.gqNursingTotal(),
            self.gqOtherInstTotal(),
            self.noninstTotal(),
            self.agecatPCO7(),
            self.agecatPCO8(),
            self.gqCollegeTotal(),
            self.gqMilitaryTotal(),
            self.gqOtherNoninstTotal(),
            self.agecatPCT12(),
            self.over17yearsTotal(),
            self.gqhh(),
            self.overage(89, total=True),
            self.underage(30, total=True),
            self.age_lessthan15(),
            self.age_greaterthan25(),
            self.age_lessthan20(),
            self.age_lt17gt65(),
            self.age_lt16gt65(),
            self.hhgq_1(),
            self.hhgq_2(),
            self.hhgq_3(),
            self.hhgq_5(),
            self.hhgq_6(),
            self.nonvoting(),
            self.hhTotal(),
            self.hispTotal(),
            self.notHispTotal(),
            self.prefix_agecats(),
            self.range_agecats(),
            self.binarysplit_agecats(),
            self.ageGroups4(),
            self.ageGroups16(),
            self.ageGroups64(),
            self.hhgq_2level()
        ]
        recodes_to_add += self.makeAllRangeQueries()

        for recode in recodes_to_add:
            self.schema.addRecode(*recode)

        # schema.addRecode("function name", {dimension: {"label": list of values}})
        #print(self.prefix_agecats.__name__)

        for func in [self.prefix_agecats(), self.range_agecats(), self.binarysplit_agecats()]:

            name , groupings = func
            dim=list(groupings.keys())[0]
            for i , label in enumerate( groupings[dim].keys() ,start=0):
                values = groupings[dim][label]
                self.schema.addRecode(f"{name}{i}", {dim: {label: values}})

    ###############################################################################
    # DHCP_HHGQ Levels
    ###############################################################################
    @staticmethod
    def getLevels():
        leveldict = SchemaSF1Factory.getLevels()
        leveldict.pop(REL, None)
        leveldict.update(
            {
                HHGQ: {
                    "Household"                                    : [0],
                    "Correctional facilities for adults"           : [1],
                    "Juvenile facilities"                          : [2],
                    "Nursing facilities/Skilled-nursing facilities": [3],
                    "Other institutional facilities"               : [4],
                    "College/University student housing"           : [5],
                    "Military quarters"                            : [6],
                    "Other noninstitutional facilities"            : [7]
                },
                
                CITIZEN: {
                    'Non-citizen': [0],
                    'Citizen'    : [1]
                },
            }
        )

        return leveldict

    @staticmethod
    def instlevels():
        """
        Groups the HHGQ variable's GQ types into Institutional and Non-Institutional
        """
        name = "instlevels"

        groups = {HHGQ:{
                "Institutional Facility": list(range(1, 5)),
                "Non-Institutional Facility": list(range(5, 8))
            }}
        return name, groups

    @staticmethod
    def householdTotal():
        name = "householdTotal"
        groups = {HHGQ:{
                "Household": [0],
            }}
        return name, groups

    @staticmethod
    def votinglevels():
        """
        Groups the AGE variable into
            Non voting age persons
            Voting age persons
        """
        name = "votinglevels"
        groups = {AGE:{
                "Non-voting Age": list(range(18)),
                "Voting Age"    : list(range(18,116))
            }}
        return name, groups



    @staticmethod
    def hhgq_2level():
        """
        collapses the HHGQ dimension to 2 levels (household and group quarters) by
        grouping together the 7 group quarter types)
        """
        name = "hhgq_2level"
        groups = {HHGQ: {
                "Household": [0],
                "Group Quarters": list(range(1, 8))
            }}
        return name, groups


    @staticmethod
    def household():
        name = DHCP.HOUSEHOLD
        groupings = {
            HHGQ: {
                "Household": [0],
                "Group Quarters": list(range(1, 8))
            }
        }
        return name, groupings

    @staticmethod
    def onlyhousehold():
        name = DHCP.ONLYHOUSEHOLD
        groupings = {
            HHGQ: {
                "Household": [0],
            }
        }
        return name, groupings

    ###############################################################################
    # Age Recode Functions
    ###############################################################################
    @staticmethod
    def ageGroups4():
        """ Age groups in ranges of 4, part of HB Tree Workload with fanout of 4 on 116 age values """
        name = "ageGroups4"
        groups = {
                AGE: {
                    "Under 4 years" : list(range(0, 4)),
                    "4 to 7 years" : list(range(4, 8)),
                    "8 to 11 years": list(range(8, 12)),
                    "12 to 15 years": list(range(12, 16)),
                    "16 to 19 years": list(range(16, 20)),
                    "20 to 23 years": list(range(20, 24)),
                    "24 to 27 years": list(range(24, 28)),
                    "28 to 31 years": list(range(28, 32)),
                    "32 to 35 years": list(range(32, 36)),
                    "36 to 39 years": list(range(36, 40)),
                    "40 to 43 years": list(range(40, 44)),
                    "44 to 47 years": list(range(44, 48)),
                    "48 to 51 years": list(range(48, 52)),
                    "52 to 55 years": list(range(52, 56)),
                    "56 to 59 years": list(range(56, 60)),
                    "60 to 63 years": list(range(60, 64)),
                    "64 to 67 years": list(range(64, 68)),
                    "68 to 71 years": list(range(68, 72)),
                    "72 to 75 years": list(range(72, 76)),
                    "76 to 79 years": list(range(76, 80)),
                    "80 to 83 years": list(range(80, 84)),
                    "84 to 87 years": list(range(84, 88)),
                    "88 to 91 years": list(range(88, 92)),
                    "92 to 95 years": list(range(92, 96)),
                    "96 to 99 years": list(range(96, 100)),
                    "100 to 103 years": list(range(100, 104)),
                    "104 to 107 years": list(range(104, 108)),
                    "108 to 111 years": list(range(108, 112)),
                    "112 to 115 years": list(range(112, 116)),
                }
            }
        return name, groups

    @staticmethod
    def ageGroups16():
        """ Age groups in ranges of 16, part of HB Tree Workload with fanout of 4 on 116 age values """
        name = "ageGroups16"
        groups = {
                AGE: {
                    "Under 16 years" : list(range(0, 16)),
                    "16 to 31 years" : list(range(16, 32)),
                    "32 to 47 years": list(range(32, 48)),
                    "48 to 63 years": list(range(48, 64)),
                    "64 to 79 years": list(range(64, 80)),
                    "80 to 95 years": list(range(80, 96)),
                    "96 to 111 years": list(range(96, 112)),
                    "112 to 115 years": list(range(112, 116)),
                }
            }
        return name, groups

    @staticmethod
    def ageGroups64():
        """ Age groups in ranges of 64, part of HB Tree Workload with fanout of 4 on 116 age values """
        name = "ageGroups64"
        groups = {
                AGE: {
                    "Under 64 years" : list(range(0, 64)),
                    "64 to 115 years" : list(range(64, 116)),
                }
            }
        return name, groups

    @staticmethod
    def makeAllRangeQueries():
        rangeQueriesList = []
        def makeRangeQueries(start, rangeQLength):
            name = f"rangeQueries({start}, {rangeQLength})"
            groups = {
                    AGE : {
                    }
                }
            for endVal in range(start+rangeQLength,116+1,rangeQLength):
                groups[AGE][str((endVal,rangeQLength))] = list(range(endVal-rangeQLength, endVal))
            return name, groups
        
        for rangeQLength in range(1,116+1):
            for start in range(min(116-rangeQLength+1,rangeQLength)):
                rangeQueriesList.append(tuple(makeRangeQueries(start, rangeQLength)))

        """
        print(f"Range queries list being returned contains {len(rangeQueriesList)} queries:")
        for rangeQuery in rangeQueriesList:
            print('\n' + str(rangeQuery))
        """
        return rangeQueriesList

    @staticmethod
    def prefix_agecats():
        """
        Create the hierarchical age categories requested by Dan (8/8). These are for the 'prefix' queries.
        """
        name = "prefix_agecats"
        agedict = {}
        count = 0
        for j in range(116):
            k = j + 1
            if j == 0:
                label = f"p{str(count).zfill(3)}_age {j}"
            else:
                label = f"p{str(count).zfill(3)}_age {0}-{j}"

            count += 1
            agedict.update({label: list(range(0, k))})

        groupings = {AGE: agedict}

        return name, groupings


    @staticmethod
    def range_agecats():
        """
        Create the hierarchical age categories requested by Phil (8/8). These are for the 'range' queries.
        """
        name="range_agecats"
        agedict = {}
        count = 0
        for j in range(116):
            for k in range((j + 1)):
                if j == k:
                    label = f"r{str(count).zfill(3)}_age {k}"
                else:
                    label = f"r{str(count).zfill(3)}_age {k}-{j}"
                count += 1
                agedict.update({label: list(range(k, j + 1))})

        groupings = {
            AGE : agedict
        }
        return name, groupings

    @staticmethod
    def binarysplit_agecats(maxage=115, splitlist=[116, 58, 29, 15, 7, 4, 2, 1]):
        """
        Create the hierarchical age categories requested initially by Phil (8/7). These are for the 'binary split' queries.
        This buckets ages into equally sized* age groups based on the size of the age groups given in splitlist.
        The last age category will be truncated to the maximum age, given by maxage, in cases where the age range cannot be broken
        into equally sized age groups.

        Arguments:  maxage takes the maximum age in data range, 115 as of Aug. 2019.
                    splitlist takes a list of integers, where the integers indicate the number of ages to include per category.

        """
        name="binarysplit_agecats"
        agedict = {}
        count = 0
        for split in splitlist:
            start = 0
            while start < maxage:
                agelist = list(range(start, start + split))
                agelist = [age for age in agelist if age <= maxage]  # For last category, remove excess ages since they may go above maxage
                end = agelist[-1]
                if start==end:
                    label = f"bs{str(count).zfill(3)}_split{split}: age {start}"
                else:
                    label = f"bs{str(count).zfill(3)}_split{split}: age {start}-{end}"
                count += 1
                agedict.update({label: agelist})
                start = end + 1

        groupings = {
            AGE : agedict
        }
        return name, groupings

    ###############################################################################
    # SF1 Recode Functions
    ###############################################################################

    @staticmethod
    def hhTotal():
        """
        Creates total of population of households for DHCP tables, (e.g., 16)
        """
        name = "hhTotal"
        groupings = {
            HHGQ: {
                "Population in households"     : [0]
            }
        }
        return name, groupings

    @staticmethod
    def hhgq2():
        """
        collapses the HHGQ dimension to 2 levels (household and group quarters) by
        grouping together the 7 group quarter types)
        """
        name = "hhgq"
        groupings = {
            HHGQ: {
                "Household"     : [0],
                "Group Quarters": list(range(1, 8))
            }
        }
        return name, groupings

    def gqlevels(self):
        """
        subset the HHGQ variable to only grab the GQ levels
        """
        name = DHCP.GQLEVELS

        reldict = self.getLevels()[HHGQ]
        gq_range = list(range(1, 8))

        gq_groupings = sk.getSubsetTuples(reldict, gq_range)

        groupings = { HHGQ: dict(gq_groupings) }

        return name, groupings

    @staticmethod
    def institutionalized():
        """
        Groups the HHGQ variable's GQ types into Institutional and Non-Institutional
        """
        name = DHCP.INSTITUTIONALIZED

        groupings = {
            HHGQ: {
                "Institutional Facility"    : list(range(1, 5)),
                "Non-Institutional Facility": list(range(5, 8))
            }
        }
        return name, groupings

    @staticmethod
    def age_lessthan15():
        name = "age_lessthan15"
        groupings = {
            AGE: {
                "Age is <15": list(range(0,15))
            }
        }
        return name, groupings

    @staticmethod
    def age_greaterthan25():
        name = "age_greaterthan25"
        groupings = {
            AGE: {
                "Age is >25": list(range(26,116))
            }
        }
        return name, groupings
        
    @staticmethod
    def age_lessthan20():
        name = "age_lessthan20"
        groupings = {
            AGE: {
                "Age is <20": list(range(0,20))
            }
        }
        return name, groupings
        
    @staticmethod
    def age_lt17gt65():
        name = "age_lt17gt65"
        groupings = {
            AGE: {
                "Age is <17 or >65": list(range(0,17)) + list(range(66,116))
            }
        }
        return name, groupings
    
    @staticmethod
    def age_lt16gt65():
        name = "age_lt16gt65"
        groupings = {
            AGE: {
                "Age is <16 or >65 ": list(range(0,16)) + list(range(66,116))
            }
        }
        return name, groupings

    @staticmethod
    def hhgq_1():
        name = "hhgq_1"
        groupings = {
            HHGQ: {
                "HHGQ: 1=Correctional facilities for adults": [1]
            }
        }
        return name, groupings        
        
    @staticmethod
    def hhgq_2():
        name = "hhgq_2"
        groupings = {
            HHGQ: {
                "HHGQ: 2=Juvenile facilities": [2]
            }
        }
        return name, groupings        
        
    @staticmethod
    def hhgq_3():
        name = "hhgq_3"
        groupings = {
            HHGQ: {
                "HHGQ: 3=Nursing facilities/Skilled-nursing facilities": [3]
            }
        }
        return name, groupings        
        
    @staticmethod
    def hhgq_5():
        name = "hhgq_5"
        groupings = {
            HHGQ: {
                "HHGQ: 5=College/University student housing": [5]
            }
        }
        return name, groupings        
        
    @staticmethod
    def hhgq_6():
        name = "hhgq_6"
        groupings = {
            HHGQ: {
                "HHGQ: 6=Military quarters": [6]
            }
        }
        return name, groupings        
        
    ###############################################################################
    # For SF1 Constraints
    ###############################################################################
    def gqhh(self):
        name = "gqhh"

        reldict = self.getLevels()[HHGQ]

        rel_range = range(0, 1)
        rel_group = [("Household", list(rel_range))]

        gq_range = range(1, 8)
        gq_group = sk.getSubsetTuples(reldict, gq_range)

        group = rel_group + gq_group

        groupings = {
            HHGQ: dict(group)
        }
        return name, groupings

    @staticmethod
    def nonvoting():
        """
        Groups the AGE variable into
            Non voting age persons
            Voting age persons
        """
        name = "nonvoting"
        groupings = {
            AGE: {
                "Non-voting Age": list(range(18)),
            }
        }
        return name, groupings

    @staticmethod
    def agecatPCO3():
        """
        Groups AGE into 5 year bands above age 20
        """
        name = "agecatPCO3"
        groupings = {
            AGE: {
                'Under 20 years'   : list(range(0,20)),
                '20 to 24 years'   : list(range(20,25)),
                '25 to 29 years'   : list(range(25,30)),
                '30 to 34 years'   : list(range(30,35)),
                '35 to 39 years'   : list(range(35,40)),
                '40 to 44 years'   : list(range(40,45)),
                '45 to 49 years'   : list(range(45,50)),
                '50 to 54 years'   : list(range(50,55)),
                '55 to 59 years'   : list(range(55,60)),
                '60 to 64 years'   : list(range(60,65)),
                '65 to 69 years'   : list(range(65,70)),
                '70 to 74 years'   : list(range(70,75)),
                '75 to 79 years'   : list(range(75,80)),
                '80 to 84 years'   : list(range(80,85)),
                '85 years and over': list(range(85,116))
            }
        }
        return name, groupings

    @staticmethod
    def agecatPCO4():
        """
        Groups AGE into 5 year bands below age 25
        """
        name = "agecatPCO4"
        groupings = {AGE: {'Under 5 years': list(range(0, 5)), '5 to 9 years': list(range(5, 10)), '10 to 14 years': list(range(10, 15)),
            '15 to 19 years': list(range(15, 20)), '20 to 24 years': list(range(20, 25)), '25 years and over': list(range(25, 116))}}
        return name, groupings

    ###############################################################################
    # SF1 Recode Functions
    ###############################################################################
    @staticmethod
    def hhgq():
        """
        collapses the
            relationship to householder levels (0-14) into a single level
            gq levels (15-42) into a single level
        """
        name = "hhgq"
        groupings = {HHGQ: {"Household": [0], "Group Quarters": list(range(1,8))}}
        return name, groupings

    def gqlevels(self):
        """
        subset the hhgq variable to only grab the GQ levels
        """
        name = "gqlevels"

        reldict = self.getLevels()[HHGQ]
        gq_range = range(1, 8)

        gq_groupings = sk.getSubsetTuples(reldict, gq_range)

        groupings = {HHGQ: dict(gq_groupings)}

        return name, groupings

    @staticmethod
    def majorGQs():
        """
        Groups the HHGQ variable's GQ types into the major GQ categories
        """
        name = "majorGQs"

        groupings = {HHGQ: {"Correctional facilities for adults": [1], "Juvenile facilities": [2],
            "Nursing facilities/Skilled-nursing facilities": [3], "Other institutional facilities": [4],
            "College/University student housing": [5],
            "Military quarters": [6], "Other noninstitutional facilities": [7]}}
        return name, groupings

    @staticmethod
    def institutionalized():
        """
        Groups the HHGQ variable's GQ types into Institutional and Non-Institutional
        """
        name = "institutionalized"

        groupings = {HHGQ: {"Institutional Facility": list(range(1,5)), "Non-Institutional Facility": list(range(5, 8))}}
        return name, groupings

    @staticmethod
    def householder():
        """
        Query for getting the number of people in Households from the HHGQ variable
        """
        name = "householder"

        groupings = {HHGQ: {"Householder": [0]}}
        return name, groupings

    @staticmethod
    def gqTotal():
        """
        Query that gives the number of persons in GQs
        """
        name = "gqTotal"

        groupings = {HHGQ: {"Total Population in Group Quarters": list(range(1,8))}}
        return name, groupings

    @staticmethod
    def instTotal():
        """
        Query for calculating the total of the 'institutionalized population' from the HHGQ variable
        """
        name = "instTotal"
        groupings = {HHGQ: {"Institutional Population Total": list(range(1,5))}}
        return name, groupings

    @staticmethod
    def gqCorrectionalTotal():
        """
        Grabs the subset of the HHGQ variable corresponding to correctional facilities for adults and calculates the total
        """
        name = "gqCorrectionalTotal"
        groupings = {HHGQ: {"Correctional Facilities for Adults Population Total": [1]}}
        return name, groupings

    @staticmethod
    def gqJuvenileTotal():
        """
        Grabs the subset of the HHGQ variable corresponding to juvenile facilities and calculates the total
        """
        name = "gqJuvenileTotal"
        groupings = {HHGQ: {"Juvenile Facilities Population Total": [2]}}
        return name, groupings

    @staticmethod
    def gqNursingTotal():
        """
        Grabs the subset of the HHGQ variable corresponding to nursing facilities and calculates the total
        """
        name = "gqNursingTotal"
        groupings = {HHGQ: {"Nursing Facilities Population Total": [3]}}
        return name, groupings

    @staticmethod
    def gqOtherInstTotal():
        """
        Grabs the subset of the HHGQ variable corresponding to other institutional facilities and calculates the total
        """
        name = "gqOtherInstTotal"
        groupings = {HHGQ: {"Other Institutional Facilities Population Total": [4]}}
        return name, groupings

    @staticmethod
    def noninstTotal():
        """
        Query for calculating the total of the 'noninstitutionalized population' from the HHGQ variable
        """
        name = "noninstTotal"
        groupings = {HHGQ: {"Noninstitutionalized Population Total": list(range(5,8))}}
        return name, groupings

    @staticmethod
    def gqCollegeTotal():
        """
        Grabs the subset of the HHGQ variable corresponding to college/university student housing levels and calculates the total
        """
        name = "gqCollegeTotal"
        groupings = {HHGQ: {"College/University Student Housing Population Total": [5]}}
        return name, groupings

    @staticmethod
    def gqMilitaryTotal():
        """
        Grabs the subset of the HHGQ variable corresponding to military quarters housing levels and calculates the total
        """
        name = "gqMilitaryTotal"
        groupings = {HHGQ: {"Military Quarters Housing Population Total": [6]}}
        return name, groupings

    @staticmethod
    def gqOtherNoninstTotal():
        """
        Grabs the subset of the HHGQ variable corresponding to other noninstitutional facilities and calculates the total
        """
        name = "gqOtherNoninstTotal"
        groupings = {HHGQ: {"Other Noninstitutional Facilities Population Total": [7]}}
        return name, groupings
