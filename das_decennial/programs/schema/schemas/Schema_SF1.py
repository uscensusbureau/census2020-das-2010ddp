import programs.cenrace as cenrace
import programs.schema.schema as sk
import numpy as np
import das_utils
from constants import REL, SEX, AGE, HISP, CENRACE
from constants import SF1, S
from programs.schema.schemas.schema_factory import SchemaFactory


# Stub for backward compatibility
def buildSchema(path=None):
    return SchemaSF1Factory(name="SF1", dimnames=[REL, SEX, AGE, HISP, CENRACE], shape=(43, 2, 116, 2, 63), path=path).getSchema()

class SchemaSF1Factory(SchemaFactory):

    def addRecodes(self):

        for recode in self.getRecodesToAdd():
            self.schema.addRecode(*recode)

    def getRecodesToAdd(self):
        ###############################################################################
        # Recodes for SF1
        ###############################################################################
        recodes_to_add = [
            self.hhgq(),
            self.gqlevels(),
            self.agecat43(),
            self.majorGQs(),
            self.institutionalized(),
            self.numraces(),
            self.votingage(),
            self.majorRaces(),
            self.racecomb(),
            #self.whiteAny(),
            #self.blackAny(),
            #self.aianAny(),
            #self.asianAny(),
            #self.nhopiAny(),
            #self.sorAny(),
            self.agecat(),
            self.under20years(),
            self.under20yearsTotal(),
            self.under18years(),
            self.under18yearsTotal(),
            self.over64yearsTotal(),
            self.relchild(),
            self.rel32(),
            self.householder(),
            self.relgq(),
            self.gqTotal(),
            self.rel34(),
            self.whiteAlone(),
            self.blackAlone(),
            self.aianAlone(),
            self.asianAlone(),
            self.nhopiAlone(),
            self.sorAlone(),
            self.tomr(),
            self.hispTotal(),
            self.notHispTotal(),
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
            self.minorGQs(),
            self.over17yearsTotal(),
            self.children(),
            self.parents(),
            self.refhh_under_15(),
            self.gqhh(),
            self.overage(89, total=True),
            self.underage(30, total=True),
        ]
        return recodes_to_add

    ###############################################################################
    # SF1 Levels
    ###############################################################################
    @staticmethod
    def getLevels():
        leveldict = {
            REL: {
                "Householder"                                                                                              : [0],
                "Husband/Wife"                                                                                             : [1],
                "Biological Son/Daughter"                                                                                  : [2],
                "Adopted Son/Daughter"                                                                                     : [3],
                "Stepson/Stepdaughter"                                                                                     : [4],
                "Brother/Sister"                                                                                           : [5],
                "Father/Mother"                                                                                            : [6],
                "Grandchild"                                                                                               : [7],
                "Parent-in-law"                                                                                            : [8],
                "Son/Daughter-in-law"                                                                                      : [9],
                "Other relative"                                                                                           : [10],
                "Roomer, Boarder"                                                                                          : [11],
                "Housemate, Roommate"                                                                                      : [12],
                "Unmarried Partner"                                                                                        : [13],
                "Other Non-relative"                                                                                       : [14],
                "Federal Detention Centers (101)"                                                                          : [15],
                "Federal Prisons (102)"                                                                                    : [16],
                "State Prisons (103)"                                                                                      : [17],
                "Local Jails and Other Municipal Confinement Facilities (104)"                                             : [18],
                "Correctional Residential Facilities (105)"                                                                : [19],
                "Military Disciplinary Barracks and Jails (106)"                                                           : [20],
                "Group Homes for Juveniles (Non-Correctional) (201)"                                                       : [21],
                "Residential Treatment Centers (Non-Correctional) (202)"                                                   : [22],
                "Correctional Facilities Intended for Juveniles (203)"                                                     : [23],
                "Nursing Facilities/Skilled-Nursing Facilities (301)"                                                      : [24],
                "Mental (Psychiatric) Hospitals and Psychiatric Units in Other Hospitals (401)"                            : [25],
                "Hospitals with Patients Who Have No Usual Home Elsewhere (402)"                                           : [26],
                "In-Patient Hospice Facilities (403)"                                                                      : [27],
                "Military Treatment Facilities with Assigned Patients (404)"                                               : [28],
                "Residential Schools for People with Disabilities (405)"                                                   : [29],
                "College/University Student Housing (501)"                                                                 : [30],
                "Military Quarters (601)"                                                                                  : [31],
                "Military Ships (602)"                                                                                     : [32],
                "Emergency and Transitional Shelters (with Sleeping Facilities) for People Experiencing Homelessness (701)": [33],
                "Soup Kitchens (702)"                                                                                      : [34],
                "Regularly Scheduled Mobile Food Vans (704)"                                                               : [35],
                "Targeted Non-Sheltered Outdoor Locations (706)"                                                           : [36],
                "Group Homes Intended for Adults (801)"                                                                    : [37],
                "Residential Treatment Centers for Adults (802)"                                                           : [38],
                "Maritime/Merchant Vessels (900)"                                                                          : [39],
                "Workers' Group Living Quarters and Job Corps Centers (901)"                                               : [40],
                "Living Quarters for Victims of Natural Disasters (903)"                                                   : [41],
                "Religious Group Quarters and Domestic Violence Shelters (904)"                                            : [42]
            },

            SEX: {
                'Male'  : [0],
                'Female': [1]
            },

            AGE: {
                'Under 1 year': [0],
                '1 year'      : [1],
                '2 years'     : [2],
                '3 years'     : [3],
                '4 years'     : [4],
                '5 years'     : [5],
                '6 years'     : [6],
                '7 years'     : [7],
                '8 years'     : [8],
                '9 years'     : [9],
                '10 years'    : [10],
                '11 years'    : [11],
                '12 years'    : [12],
                '13 years'    : [13],
                '14 years'    : [14],
                '15 years'    : [15],
                '16 years'    : [16],
                '17 years'    : [17],
                '18 years'    : [18],
                '19 years'    : [19],
                '20 years'    : [20],
                '21 years'    : [21],
                '22 years'    : [22],
                '23 years'    : [23],
                '24 years'    : [24],
                '25 years'    : [25],
                '26 years'    : [26],
                '27 years'    : [27],
                '28 years'    : [28],
                '29 years'    : [29],
                '30 years'    : [30],
                '31 years'    : [31],
                '32 years'    : [32],
                '33 years'    : [33],
                '34 years'    : [34],
                '35 years'    : [35],
                '36 years'    : [36],
                '37 years'    : [37],
                '38 years'    : [38],
                '39 years'    : [39],
                '40 years'    : [40],
                '41 years'    : [41],
                '42 years'    : [42],
                '43 years'    : [43],
                '44 years'    : [44],
                '45 years'    : [45],
                '46 years'    : [46],
                '47 years'    : [47],
                '48 years'    : [48],
                '49 years'    : [49],
                '50 years'    : [50],
                '51 years'    : [51],
                '52 years'    : [52],
                '53 years'    : [53],
                '54 years'    : [54],
                '55 years'    : [55],
                '56 years'    : [56],
                '57 years'    : [57],
                '58 years'    : [58],
                '59 years'    : [59],
                '60 years'    : [60],
                '61 years'    : [61],
                '62 years'    : [62],
                '63 years'    : [63],
                '64 years'    : [64],
                '65 years'    : [65],
                '66 years'    : [66],
                '67 years'    : [67],
                '68 years'    : [68],
                '69 years'    : [69],
                '70 years'    : [70],
                '71 years'    : [71],
                '72 years'    : [72],
                '73 years'    : [73],
                '74 years'    : [74],
                '75 years'    : [75],
                '76 years'    : [76],
                '77 years'    : [77],
                '78 years'    : [78],
                '79 years'    : [79],
                '80 years'    : [80],
                '81 years'    : [81],
                '82 years'    : [82],
                '83 years'    : [83],
                '84 years'    : [84],
                '85 years'    : [85],
                '86 years'    : [86],
                '87 years'    : [87],
                '88 years'    : [88],
                '89 years'    : [89],
                '90 years'    : [90],
                '91 years'    : [91],
                '92 years'    : [92],
                '93 years'    : [93],
                '94 years'    : [94],
                '95 years'    : [95],
                '96 years'    : [96],
                '97 years'    : [97],
                '98 years'    : [98],
                '99 years'    : [99],
                '100 years'   : [100],
                '101 years'   : [101],
                '102 years'   : [102],
                '103 years'   : [103],
                '104 years'   : [104],
                '105 years'   : [105],
                '106 years'   : [106],
                '107 years'   : [107],
                '108 years'   : [108],
                '109 years'   : [109],
                '110 years'   : [110],
                '111 years'   : [111],
                '112 years'   : [112],
                '113 years'   : [113],
                '114 years'   : [114],
                '115 years'   : [115]
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
            }
        }

        return leveldict

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
        name = S.HHGQ
        groupings = {
            REL: {
                "Household"     : list(range(0,15)),
                "Group Quarters": list(range(15,43))
            }
        }
        return name, groupings

    def gqlevels(self):
        """
        subset the rel variable to only grab the GQ levels
        """
        name = SF1.GQLEVELS

        reldict = self.getLevels()[REL]
        gq_range = range(15,43)

        gq_groupings = sk.getSubsetTuples(reldict, gq_range)

        groupings = { REL: dict(gq_groupings) }

        return name, groupings

    @staticmethod
    def agecat43():
        """
        for table P43, recode the age variable to reflect the following age groups
            Under 18
            18 to 64
            65 and over
        """
        name = SF1.AGECAT43

        groupings = {
            AGE: {
                "Under 18 years"   : list(range(0,18)),
                "18 to 64 years"   : list(range(18,65)),
                "65 years and over": list(range(65,116))
            }
        }
        return name, groupings

    @staticmethod
    def agegroup4():
        """ Age groups in ranges of 4, part of HB Tree Workload with fanout of 4 on 116 age values """
        name = SF1.AGEGROUP4
        groupings = {
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
        return name, groupings

    @staticmethod
    def agegroup16():
        """ Age groups in ranges of 16, part of HB Tree Workload with fanout of 4 on 116 age values """
        name = SF1.AGEGROUP16
        groupings = {
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
        return name, groupings

    @staticmethod
    def agegroup64():
        """ Age groups in ranges of 64, part of HB Tree Workload with fanout of 4 on 116 age values """
        name = SF1.AGEGROUP64
        groupings = {
            AGE: {
                "Under 64 years" : list(range(0, 64)),
                "64 to 115 years" : list(range(64, 116)),
            }
        }
        return name, groupings



    @staticmethod
    def majorGQs():
        """
        Groups the REL variable's GQ types into the major GQ categories

        Note that in the REL variable, the GQ 502 level is missing
            Is this a typo?
        """
        name = SF1.MAJORGQS

        groupings = {
            REL: {
                "Correctional facilities for adults"           : list(range(15,21)),
                "Juvenile facilities"                          : list(range(21,24)),
                "Nursing facilities/Skilled-nursing facilities": list(range(24,25)),
                "Other institutional facilities"               : list(range(25,30)),
                "College/University student housing"           : list(range(30,31)), # GQ 502 is missing???
                "Military quarters"                            : list(range(31,33)),
                "Other noninstitutional facilities"            : list(range(33,43))
            }
        }
        return name, groupings

    @staticmethod
    def gqCatPCT21():
        """
        GQ categories required by PCT21. More detailed than major GQ cat, but still groups some GQ together
        """
        name = SF1.GQCATPCT21
        groupings = {
            REL: {
                "Federal Detention Centers (101)"                                                                          : [15],
                "Federal Prisons (102)"                                                                                    : [16],
                "State Prisons (103)"                                                                                      : [17],
                "Local Jails and Other Municipal Confinement Facilities (104)"                                             : [18],
                "Correctional Residential Facilities (105)"                                                                : [19],
                "Military Disciplinary Barracks and Jails (106)"                                                           : [20],
                "Group Homes for Juveniles (Non-Correctional) (201)"                                                       : [21],
                "Residential Treatment Centers (Non-Correctional) (202)"                                                   : [22],
                "Correctional Facilities Intended for Juveniles (203)"                                                     : [23],
                "Nursing Facilities/Skilled-Nursing Facilities (301)"                                                      : [24],
                "Mental (Psychiatric) Hospitals and Psychiatric Units in Other Hospitals (401)"                            : [25],
                "Hospitals with Patients Who Have No Usual Home Elsewhere (402)"                                           : [26],
                "In-Patient Hospice Facilities (403)"                                                                      : [27],
                "Military Treatment Facilities with Assigned Patients (404)"                                               : [28],
                "Residential Schools for People with Disabilities (405)"                                                   : [29],
                "College/University Student Housing (501-502)"                                                                 : [30], # 502 is missing, why?
                "Military Quarters (601)"                                                                                  : [31],
                "Military Ships (602)"                                                                                     : [32],
                "Emergency and Transitional Shelters (with Sleeping Facilities) for People Experiencing Homelessness (701)": [33],
                "Group Homes Intended for Adults (801)"                                                                    : [37],
                "Residential Treatment Centers for Adults (802)"                                                           : [38],
                "Maritime/Merchant Vessels (900)"                                                                          : [39],
                "Workers' Group Living Quarters and Job Corps Centers (901)"                                               : [40],
                "Other noninstitutional facilities (702, 704, 706, 903-904)"                                               : [34, 35, 36, 41, 42],
            }
        }
        return name, groupings

    @staticmethod
    def institutionalized():
        """
        Groups the REL variable's GQ types into Institutional and Non-Institutional
        """
        name = SF1.INSTITUTIONALIZED

        groupings = {
            REL: {
                "Institutional Facility"    : list(range(15,30)),
                "Non-Institutional Facility": list(range(30,43))
            }
        }
        return name, groupings

    @staticmethod
    def numraces():
        """
        Group CENRACE levels by the number of races present in the level
        """
        name = SF1.NUMRACES

        group = cenrace.getCenraceNumberGroups()
        labels = group.getGroupLabels()
        indices = group.getIndexGroups()

        groupings = {
            CENRACE: dict(zip(labels, indices))
        }
        return name, groupings

    @staticmethod
    def votingage():
        """
        Groups the AGE variable into
            Non voting age persons
            Voting age persons
        """
        name = SF1.VOTINGAGE
        groupings = {
            AGE: {
                "Non-voting Age": list(range(18)),
                "Voting Age"    : list(range(18,116))
            }
        }
        return name, groupings

    @staticmethod
    def majorRaces():
        """
        Group CENRACE into individual single-race categories (alone) and another level that represents all combinations of races
        """
        name = SF1.MAJORRACES
        groupings = {
            CENRACE: {
                "White alone": [0],
                "Black or African American alone": [1],
                "American Indian and Alaska Native alone": [2],
                "Asian alone": [3],
                "Native Hawaiian and Other Pacific Islander alone": [4],
                "Some Other Race alone": [5],
                "Two or More Races": list(range(6,63))
            }
        }
        return name, groupings

    #@staticmethod
    #def racecombTotal():
        #"""
        #Not currently used since we need a new Query class to handle it properly
        #"""
        #name = "racecombTotal"

        #racecomb_groups = cenrace.getCenraceAloneOrInCombinationGroups().getIndexGroups()

        #groupings = {
            #CENRACE: {
                #"Total Races Tallied": np.array(racecomb_groups).flatten().tolist()
            #}
        #}
        #return name, groupings

    @staticmethod
    def racecomb():
        """

        """
        name = SF1.RACECOMB

        labels = cenrace.getCenraceAloneOrInCombinationGroups().getGroupLabels()
        groups = cenrace.getCenraceAloneOrInCombinationGroups().getIndexGroups()

        groupings = {
            CENRACE: {
                labels[0]: groups[0]
            }
        }
        return name, groupings

    @staticmethod
    def whiteAny(self):
        """
        Create white along or incombination as a non-exclusive race category for use in tables like P6 and P7.
        """
        name = "whiteAny"

        #Find the list of cenrace categories that include the function
        racetxt = name.replace('Any', '')
        labels = cenrace.getCenraceAloneOrInCombinationGroups().getGroupLabels()
        groups = cenrace.getCenraceAloneOrInCombinationGroups().getIndexGroups()

        idx = [racetxt in label for label in labels]
        label = labels[idx]
        cats = groups[idx]

        groupings = {
            CENRACE: {
                label: cats
            }
        }
        return name, groupings

    @staticmethod
    def blackAny(self):
        """
        Create white along or incombination as a non-exclusive race category for use in tables like P6 and P7.
        """
        name = "whiteAny"

        #Find the list of cenrace categories that include the function
        racetxt = name.replace('Any', '')
        labels = cenrace.getCenraceAloneOrInCombinationGroups().getGroupLabels()
        groups = cenrace.getCenraceAloneOrInCombinationGroups().getIndexGroups()

        idx = [racetxt in label for label in labels]
        label = labels[idx]
        cats = groups[idx]

        groupings = {
            CENRACE: {
                label: cats
            }
        }
        return name, groupings

    @staticmethod
    def aianAny(self):
        """
        Create aian along or incombination as a non-exclusive race category for use in tables like P6 and P7.
        """
        name = "aianAny"

        #Find the list of cenrace categories that include the function
        racetxt = name.replace('Any', '')
        labels = cenrace.getCenraceAloneOrInCombinationGroups().getGroupLabels()
        groups = cenrace.getCenraceAloneOrInCombinationGroups().getIndexGroups()

        idx = [racetxt in label for label in labels]
        label = labels[idx]
        cats = groups[idx]

        groupings = {
            CENRACE: {
                label: cats
            }
        }
        return name, groupings

    @staticmethod
    def asianAny(self):
        """
        Create asian along or incombination as a non-exclusive race category for use in tables like P6 and P7.
        """
        name = "asianAny"

        #Find the list of cenrace categories that include the function
        racetxt = name.replace('Any', '')
        labels = cenrace.getCenraceAloneOrInCombinationGroups().getGroupLabels()
        groups = cenrace.getCenraceAloneOrInCombinationGroups().getIndexGroups()

        idx = [racetxt in label for label in labels]
        label = labels[idx]
        cats = groups[idx]

        groupings = {
            CENRACE: {
                label: cats
            }
        }
        return name, groupings

    @staticmethod
    def nhopiAny(self):
        """
        Create nhopi along or in combination as a non-exclusive race category for use in tables like P6 and P7.
        """
        name = "nhopiAny"

        #Find the list of cenrace categories that include the function
        racetxt = name.replace('Any', '')
        labels = cenrace.getCenraceAloneOrInCombinationGroups().getGroupLabels()
        groups = cenrace.getCenraceAloneOrInCombinationGroups().getIndexGroups()

        idx = [racetxt in label for label in labels]
        label = labels[idx]
        cats = groups[idx]

        groupings = {
            CENRACE: {
                label: cats
            }
        }
        return name, groupings

    @staticmethod
    def sorAny(self):
        """
        Create sor along or in combination as a non-exclusive race category for use in tables like P6 and P7.
        """
        name = "sorAny"

        #Find the list of cenrace categories that include the function
        racetxt = name.replace('Any', '')
        labels = cenrace.getCenraceAloneOrInCombinationGroups().getGroupLabels()
        groups = cenrace.getCenraceAloneOrInCombinationGroups().getIndexGroups()

        idx = [racetxt in label for label in labels]
        label = labels[idx]
        cats = groups[idx]

        groupings = {
            CENRACE: {
                label: cats
            }
        }
        return name, groupings


    ''' Race any, categorized individually
    @staticmethod
    def whiteAny():
        """
        Create white along or incombination as a non-exclusive race category for use in tables like P6 and P7.
        """
        name = "whiteAny"

        # Find the list of cenrace categories that include the function
        racetxt = name.replace('Any', '')
        labels = cenrace.getCenraceAloneOrInCombinationGroups().getGroupLabels()
        groups = cenrace.getCenraceAloneOrInCombinationGroups().getIndexGroups()

        idx = [racetxt in label for label in labels]
        idx = [idx.index(i) for i in idx if i == True][0]
        label = labels[0]
        cats = groups[0]

        groupings = {CENRACE: {label: cats}}
        return name, groupings

    @staticmethod
    def blackAny():
        """
        Create white along or incombination as a non-exclusive race category for use in tables like P6 and P7.
        """
        name = "blackAny"

        # Find the list of cenrace categories that include the function
        racetxt = name.replace('Any', '')
        labels = cenrace.getCenraceAloneOrInCombinationGroups().getGroupLabels()
        groups = cenrace.getCenraceAloneOrInCombinationGroups().getIndexGroups()

        idx = [racetxt in label for label in labels]
        idx = [idx.index(i) for i in idx if i == True][0]
        label = labels[idx]
        cats = groups[idx]

        groupings = {CENRACE: {label: cats, 'total races': list(range(0,63))}}
        return name, groupings

    @staticmethod
    def aianAny():
        """
        Create aian along or incombination as a non-exclusive race category for use in tables like P6 and P7.
        """
        name = "aianAny"

        # Find the list of cenrace categories that include the function
        racetxt = name.replace('Any', '')
        labels = cenrace.getCenraceAloneOrInCombinationGroups().getGroupLabels()
        groups = cenrace.getCenraceAloneOrInCombinationGroups().getIndexGroups()

        idx = [racetxt in label for label in labels]
        idx = [idx.index(i) for i in idx if i == True][0]
        label = labels[idx]
        cats = groups[idx]

        groupings = {CENRACE: {label: cats}}
        return name, groupings

    @staticmethod
    def asianAny():
        """
        Create asian along or incombination as a non-exclusive race category for use in tables like P6 and P7.
        """
        name = "asianAny"

        # Find the list of cenrace categories that include the function
        racetxt = name.replace('Any', '')
        labels = cenrace.getCenraceAloneOrInCombinationGroups().getGroupLabels()
        groups = cenrace.getCenraceAloneOrInCombinationGroups().getIndexGroups()

        idx = [racetxt in label for label in labels]
        idx = [idx.index(i) for i in idx if i == True][0]
        label = labels[idx]
        cats = groups[idx]

        groupings = {CENRACE: {label: cats}}
        return name, groupings

    @staticmethod
    def nhopiAny():
        """
        Create nhopi along or in combination as a non-exclusive race category for use in tables like P6 and P7.
        """
        name = "nhopiAny"

        # Find the list of cenrace categories that include the function
        racetxt = name.replace('Any', '')
        labels = cenrace.getCenraceAloneOrInCombinationGroups().getGroupLabels()
        groups = cenrace.getCenraceAloneOrInCombinationGroups().getIndexGroups()

        idx = [racetxt in label for label in labels]
        idx = [idx.index(i) for i in idx if i == True][0]
        label = labels[idx]
        cats = groups[idx]

        groupings = {CENRACE: {label: cats}}
        return name, groupings

    @staticmethod
    def sorAny():
        """
        Create sor along or in combination as a non-exclusive race category for use in tables like P6 and P7.
        """
        name = "sorAny"

        # Find the list of cenrace categories that include the function
        racetxt = name.replace('Any', '')
        labels = cenrace.getCenraceAloneOrInCombinationGroups().getGroupLabels()
        groups = cenrace.getCenraceAloneOrInCombinationGroups().getIndexGroups()

        idx = [racetxt in label for label in labels]
        idx = [idx.index(i) for i in idx if i == True][0]
        label = labels[idx]
        cats = groups[idx]

        groupings = {CENRACE: {label: cats}}
        return name, groupings
    '''

    @staticmethod
    def agecat():
        """
        Group AGE levels into specific age-bands
        """
        name = SF1.AGECAT
        groupings = {
            AGE: {
                'Under 5 years'    : list(range(0,5)),
                '5 to 9 years'     : list(range(5,10)),
                '10 to 14 years'   : list(range(10,15)),
                '15 to 17 years'   : list(range(15,18)),
                '18 and 19 years'  : list(range(18,20)),
                '20 years'         : list(range(20,21)),
                '21 years'         : list(range(21,22)),
                '22 to 24 years'   : list(range(22,25)),
                '25 to 29 years'   : list(range(25,30)),
                '30 to 34 years'   : list(range(30,35)),
                '35 to 39 years'   : list(range(35,40)),
                '40 to 44 years'   : list(range(40,45)),
                '45 to 49 years'   : list(range(45,50)),
                '50 to 54 years'   : list(range(50,55)),
                '55 to 59 years'   : list(range(55,60)),
                '60 and 61 years'  : list(range(60,62)),
                '62 to 64 years'   : list(range(62,65)),
                '65 and 66 years'  : list(range(65,67)),
                '67 to 69 years'   : list(range(67,70)),
                '70 to 74 years'   : list(range(70,75)),
                '75 to 79 years'   : list(range(75,80)),
                '80 to 84 years'   : list(range(80,85)),
                '85 years and over': list(range(85,116))
            }
        }
        return name, groupings


    def under20years(self):
        """
        Subsets AGE to only focus on the ages 0 to 19
        """
        name = "under20years"

        agedict = self.getLevels()[AGE]

        age_groupings = sk.getSubsetTuples(agedict, range(0,20))

        groupings = {
            AGE: dict(age_groupings)
        }
        return name, groupings

    @staticmethod
    def under20yearsTotal():
        """
        Query for calculating the total for the 0 to 19 subset of AGE
        """
        name = "under20yearsTotal"

        groupings = {
            AGE: {
                "Total Population Under 20 Years Old": list(range(0,20))
            }
        }
        return name, groupings

    @staticmethod
    def under18years():
        """
        Groups the AGE variable's 0 to 17 subset into specific categories
        """
        name = "under18years"

        groupings = {
            AGE: {
                "Under 3 years"  : list(range(0,3)),
                "3 and 4 years"  : list(range(3,5)),
                "5 years"        : list(range(5,6)),
                "6 to 11 years"  : list(range(6,12)),
                "12 and 13 years": list(range(12,14)),
                "14 years"       : list(range(14,15)),
                "15 to 17 years" : list(range(15,18))
            }
        }
        return name, groupings

    @staticmethod
    def under18yearsTotal():
        """
        Query for calculating the total for the 0 to 17 subset of AGE
        """
        name = "under18yearsTotal"
        groupings = {
            AGE: {
                "Total Population Under 18 Years": list(range(0,18))
            }
        }
        return name, groupings

    @staticmethod
    def over64yearsTotal():
        """
        Query for calculating the total for the 65+ subset of AGE
        """
        name = "over64yearsTotal"

        groupings = {
            AGE: {
                "65 years or older": list(range(65,116))
            }
        }
        return name, groupings

    @staticmethod
    def relchild():
        """
        Groups the REL variable into two categories
            Own child
            Other children

        Note: Which levels map into these categories?

        According to the email response from Rose Kreider on
        20 Feb 2019:
        All children < 18 years who live in households should be classified
        into one of three categories:

        1. Householder, spouse, or partner
        2. Own child, which refers to Biological, Step, or Adopted children < 18 years of the householder.
        3. Other child, which refers to all other non group quarters levels, even though some levels are not possible for people < 18 years (e.g. parent of the householder)
        """
        name = "relchild"

        groupings = {
            REL: {
                "Own child"     : [2,3,4],
                "Other children": [5,6,7,8,9,10,11,12,14]
            }
        }
        return name, groupings

    @staticmethod
    def rel32():
        """
        Groups REL variable into categories specific to table P32

        Note: Still need to figure out the details of what "own child" and
        "other children" mean.

        Note: Assuming that "other children" includes all levels not connected to
            "householder, spouse, unmarried partner",
            "own children" - "biological", "adopted", "step"
            "institutional gq",
            "noninstitutional gq"
        This includes levels such as "parent", "son/daughter in law", "grandchild", etc.

        According to the email response from Rose Kreider on
        20 Feb 2019:
        All children < 18 years who live in households should be classified
        into one of three categories:

        1. Householder, spouse, or partner
        2. Own child, which refers to Biological, Step, or Adopted children < 18 years of the householder.
        3. Other child, which refers to all other non group quarters levels, even though some levels are not possible for people < 18 years (e.g. parent of the householder)
        """
        name = "rel32"

        groupings = {
            REL: {
                "Householder, spouse, or unmarried partner": [0,1,13],
                "Own child"                                : [2,3,4],
                "Other children"                           : [5,6,7,8,9,10,11,12,14],
                "Institutionalized Population"             : list(range(15,30)),
                "Noninstitutionalized Population"          : list(range(30,43))
            }
        }
        return name, groupings

    @staticmethod
    def householder():
        """
        Query for getting the number of householders in the REL variable
        """
        name = "householder"

        groupings = {
            REL: {
                "Householder": [0]
            }
        }
        return name, groupings

    def relgq(self):
        """
        Recode defined in P29 Table

        Note that the "Foster child" level doesn't exist in the histogram definition for the REL varaible

        Additionally, the "Living Alone/Not Living Alone" levels/variables are not defined in the histogram for the REL variable

        Groups the REL variable into
            Individual categories for each householder-relationship level
            Groups all Institutionalized categories into a single level
            Groups all Noninstitutionalized categories into a single level
        """
        name = "relgq"

        reldict = self.getLevels()[REL]

        # keep the individual relationship categories
        rel_group = sk.getSubsetTuples(reldict, range(0,15))

        # group the gq categories into two categories
        rel_group += [
            ('Institutionalized population'   , list(range(15,30))),
            ('Noninstitutionalized population', list(range(30,43)))
        ]

        groupings = {
            REL: dict(rel_group)
        }
        return name, groupings

    @staticmethod
    def gqTotal():
        """
        Query that gives the number of persons in GQs
        """
        name = SF1.GQTOTAL

        groupings = {
            REL: {
                "Total Population in Group Quarters": list(range(15,43))
            }
        }
        return name, groupings

    @staticmethod
    def rel34():
        """
        Groups the REL variable to the form specified by the P34 Table
        """
        name = "rel34"
        groupings = {
            REL: {
                "Householder, no spouse or partner": [0],
                "Spouse or partner (including householders with spouses/partners)": [1,13],
                "Parent or parent-in-law": [6,8],
                "Other relatives": [2,3,4,5,7,9,10],
                "Other nonrelatives": [11,12,14],
                "Institutionalized population": list(range(15,30)),
                "Noninstitutionalized population": list(range(30,43))
            }
        }
        return name, groupings

    @staticmethod
    def whiteAlone():
        """
        Subset the CENRACE variable to include only the "White" category, alone
        """
        name = SF1.WHITEALONE
        groupings = {
            CENRACE: {
                "White alone": [0]
            }
        }
        return name, groupings

    @staticmethod
    def blackAlone():
        """
        Subset the CENRACE variable to include only the "Black or African American" category, alone
        """
        name = "blackAlone"
        groupings = {
            CENRACE: {
                "Black or African American alone": [1]
            }
        }
        return name, groupings

    @staticmethod
    def aianAlone():
        """
        Subset the CENRACE variable to include only the "American Indian and Alaska Native" category, alone
        """
        name = "aianAlone"
        groupings = {
            CENRACE: {
                "American Indian and Alaska Native alone": [2]
            }
        }
        return name, groupings

    @staticmethod
    def asianAlone():
        """
        Subset the CENRACE variable to include only the "Asian" category, alone
        """
        name = "asianAlone"
        groupings = {
            CENRACE: {
                "Asian alone": [3]
            }
        }
        return name, groupings

    @staticmethod
    def nhopiAlone():
        """
        Subset the CENRACE variable to include only the "Native Hawaiian and Pacific Islander" category, alone
        """
        name = "nhopiAlone"
        groupings = {
            CENRACE: {
                "Native Hawaiian and Pacific Islander alone": [4]
            }
        }
        return name, groupings

    @staticmethod
    def sorAlone():
        """
        Subset the CENRACE variable to include only the "Some Other Race" category, alone
        """
        name = "sorAlone"
        groupings = {
            CENRACE: {
                "Some Other Race alone": [5]
            }
        }
        return name, groupings

    @staticmethod
    def tomr():
        """
        Subset the CENRACE variable to include only the "Two or more races" category, alone
        """
        name = "tomr"
        groupings = {
            CENRACE: {
                "Two or more races": list(range(6,63))
            }
        }
        return name, groupings

    @staticmethod
    def hispTotal():
        """
        Subset of the HISP variable to include only the "Hispanic or Latino" category
        """
        name = SF1.HISPTOTAL
        groupings = {
            HISP: {
                "Hispanic or Latino": [1]
            }
        }
        return name, groupings

    @staticmethod
    def notHispTotal():
        """
        Subset of the HISP variable to include only the "Not Hispanic or Latino" category
        """
        name = SF1.NOTHISPTOTAL
        groupings = {
            HISP: {
                "Not Hispanic or Latino": [0]
            }
        }
        return name, groupings

    @staticmethod
    def agecatPCO1():
        """
        Groups AGE into 5 year bands
        """
        name = "agecatPCO1"
        groupings = {AGE: {'Under 5 years': list(range(0, 5)), '5 to 9 years': list(range(5, 10)), '10 to 14 years': list(range(10, 15)),
            '15 to 19 years': list(range(15, 20)), '20 to 24 years': list(range(20, 25)), '25 to 29 years': list(range(25, 30)),
            '30 to 34 years': list(range(30, 35)), '35 to 39 years': list(range(35, 40)), '40 to 44 years': list(range(40, 45)),
            '45 to 49 years': list(range(45, 50)), '50 to 54 years': list(range(50, 55)), '55 to 59 years': list(range(55, 60)),
            '60 to 64 years': list(range(60, 65)), '65 to 69 years': list(range(65, 70)), '70 to 74 years': list(range(70, 75)),
            '75 to 79 years': list(range(75, 80)), '80 to 84 years': list(range(80, 85)), '85 years and over': list(range(85, 116))}}
        return name, groupings

    @staticmethod
    def agecat_5yr():
        """
        Groups AGE into 5 year bands
        """
        name = "agecat_5yr"
        groupings = {
            AGE: {
                'Under 5 years'    : list(range(0,5)),
                '5 to 9 years'     : list(range(5,10)),
                '10 to 14 years'   : list(range(10,15)),
                '15 to 19 years'   : list(range(15,20)),
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
    def instTotal():
        """
        Query for calculating the total of the 'institutionalized population' from the REL variable
        """
        name = "instTotal"
        groupings = {
            REL: {
                "Institutional Population Total": list(range(15,30))
            }
        }
        return name, groupings

    @staticmethod
    def gqCorrectionalTotal():
        """
        Grabs the subset of the REL variable corresponding to correctional facilities for adults and calculates the total
        """
        name = "gqCorrectionalTotal"
        groupings = {
            REL: {
                "Correctional Facilities for Adults Population Total": list(range(15,21))
            }
        }
        return name, groupings

    @staticmethod
    def gqJuvenileTotal():
        """
        Grabs the subset of the REL variable corresponding to juvenile facilities and calculates the total
        """
        name = "gqJuvenileTotal"
        groupings = {
            REL: {
                "Juvenile Facilities Population Total": list(range(21,24))
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

    @staticmethod
    def agecatPCO5():
        """
        Groups the AGE variable according to the specifics of the PCO5 Table (Nursing population)
        """
        name = "agecatPCO5"
        groupings = {
            AGE: {
                'Under 30 years'   : list(range(0,30)),
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
    def gqNursingTotal():
        """
        Grabs the subset of the REL variable corresponding to nursing facilities and calculates the total
        """
        name = "gqNursingTotal"
        groupings = {
            REL: {
                "Nursing Facilities Population Total": [24]
            }
        }
        return name, groupings

    @staticmethod
    def gqOtherInstTotal():
        """
        Grabs the subset of the REL variable corresponding to other institutional facilities and calculates the total
        """
        name = "gqOtherInstTotal"
        groupings = {
            REL: {
                "Other Institutional Facilities Population Total": list(range(25,30))
            }
        }
        return name, groupings

    @staticmethod
    def noninstTotal():
        """
        Query for calculating the total of the 'noninstitutionalized population' from the REL variable
        """
        name = "noninstTotal"
        groupings = {
            REL: {
                "Noninstitutionalized Population Total": list(range(30,43))
            }
        }
        return name, groupings

    @staticmethod
    def agecatPCO7():
        """
        Recodes AGE into bands specified in Table PCO7
        """
        name = "agecatPCO7"
        groupings = {
            AGE: {
                'Under 15 years'   : list(range(0,15)),
                '15 to 19 years'   : list(range(15,20)),
                '20 to 24 years'   : list(range(20,25)),
                '25 to 29 years'   : list(range(25,30)),
                '30 to 34 years'   : list(range(30,35)),
                '35 to 39 years'   : list(range(35,40)),
                '40 to 44 years'   : list(range(40,45)),
                '45 to 49 years'   : list(range(45,50)),
                '50 to 54 years'   : list(range(50,55)),
                '55 to 59 years'   : list(range(55,60)),
                '60 to 64 years'   : list(range(60,65)),
                '65 years and over': list(range(65,116))
            }
        }
        return name, groupings

    @staticmethod
    def agecatPCO8():
        """
        Recodes AGE into bands specified in Table PCO8
        """
        name = "agecatPCO8"
        groupings = {
            AGE: {
                "Under 20 years"   : list(range(0,20)),
                "20 to 24 years"   : list(range(20,25)),
                "25 to 29 years"   : list(range(25,30)),
                "30 to 39 years"   : list(range(30,40)),
                "40 years and over": list(range(40,116))
            }
        }
        return name, groupings

    @staticmethod
    def gqCollegeTotal():
        """
        Grabs the subset of the REL variable corresponding to college/university student housing levels and calculates the total

        Note: GQ 502 is missing from the histogram... is this intentional or a typo?
        """
        name = "gqCollegeTotal"
        groupings = {
            REL: {
                "College/University Student Housing Population Total": [30]
            }
        }
        return name, groupings

    @staticmethod
    def gqMilitaryTotal():
        """
        Grabs the subset of the REL variable corresponding to military quarters housing levels and calculates the total
        """
        name = "gqMilitaryTotal"
        groupings = {
            REL: {
                "Military Quarters Housing Population Total": list(range(31,33))
            }
        }
        return name, groupings

    @staticmethod
    def gqOtherNoninstTotal():
        """
        Grabs the subset of the REL variable corresponding to other noninstitutional facilities and calculates the total
        """
        name = "gqOtherNoninstTotal"
        groupings = {
            REL: {
                "Other Noninstitutional Facilities Population Total": list(range(33,43))
            }
        }
        return name, groupings

    def agecatPCT12(self):
        """
        Groups ages as specified in the PCT12 table
        """
        name = "agecatPCT12"

        agedict = self.getLevels()[AGE]

        single_age_group_0_to_100 = sk.getSubsetTuples(agedict, range(0,100))
        other_age_groups = [
            ( "100 to 104 years"  , list(range(100,105)) ),
            ( "105 to 109 years"  , list(range(105,110)) ),
            ( "110 years and over", list(range(110,116)) ),
        ]

        age_groupings = single_age_group_0_to_100 + other_age_groups

        groupings = {
            AGE: dict(age_groupings)
        }
        return name, groupings

    def minorGQs(self):
        """
        Breaks down the GQ types into their finest levels
        See Table PCT21 for more details
        """
        name = "minorGQs"

        reldict = self.getLevels()[REL]
        rel_range  = range(15,34)
        rel_range2 = range(37,41)

        rel_groupings = sk.getSubsetTuples(reldict, rel_range)
        rel_groupings += sk.getSubsetTuples(reldict, rel_range2)
        rel_groupings += [
            ("Other noninstitutional facilities (702, 704, 706, 903-904)", [34,35,36,41,42])
        ]

        groupings = {
            REL: dict(rel_groupings)
        }
        return name, groupings

    @staticmethod
    def over17yearsTotal():
        """
        Subsets the AGE variable to include only the ages 18 and over
        """
        name = "over17yearsTotal"
        groupings = {
            AGE: {
                "18 years or older": list(range(17,116))
            }
        }
        return name, groupings

    ###############################################################################
    # For SF1 Constraints
    ###############################################################################
    @staticmethod
    def children():
        name = "children"
        groupings = {
            REL: {
                'Child': [2,3,4,9] #TODO: Should 7 (Grandchild) also be included?
            }
        }
        return name, groupings

    @staticmethod
    def parents():
        name = "parents"
        groupings = {
            REL: {
                'Parent': [6,8]
            }
        }
        return name, groupings

    @staticmethod
    def refhh_under_15():
        name = "refhhunder15"
        groupings = {
            REL: {
                'RefHH': [0,1,6,8,9,12,13]
            },

            AGE: {
                'Under 15': list(range(15))
            }
        }
        return name, groupings

    def gqhh(self):
        name = "gqhh"

        reldict = self.getLevels()[REL]

        rel_range = range(0,15)
        rel_group = [ ("Household", list(rel_range)) ]

        gq_range = range(15,43)
        gq_group = sk.getSubsetTuples(reldict, gq_range)

        group = rel_group + gq_group

        groupings = {
            REL: dict(group)
        }
        return name, groupings

    ###############################################################################
    # Possible useful functions for generating common recode types
    ###############################################################################
    def agerange(self, lower, upper, name=None, total=False):
        if name is not None:
            name = name
        else:
            name = f"age_{lower}_to_{upper}"
            if total:
                name += "_total"

        agedict = self.getLevels()[AGE]
        age_range = range(lower, upper+1)

        if total:
            groupings = {
                AGE: {
                    f"Total in Age Range ({lower} to {upper})": list(age_range)
                }
            }

        else:
            age_groupings = sk.getSubsetTuples(agedict, age_range)
            groupings = {
                AGE: dict(age_groupings)
            }

        return name, groupings


    def overage(self, age, total=False):
        name = f"over{age}years"

        agedict = self.getLevels()[AGE]
        maxage = max([x[0] for x in agedict.values()])
        age_range = range(age+1, maxage+1)

        if total:
            name = f"{name}Total"
            age_groupings = [(f"Total Population {age+1} years or older", list(age_range))]

        else:
            age_groupings = sk.getSubsetTuples(agedict, age_range)

        groupings = {
            AGE: dict(age_groupings)
        }
        return name, groupings


    def underage(self, age, total=False):
        name = f"under{age}years"

        agedict = self.getLevels()[AGE]
        age_range = range(0,age)

        if total:
            name = f"{name}Total"
            age_groupings = [(f"Total Population Under {age} years old", list(age_range))]

        else:
            age_groupings = sk.getSubsetTuples(agedict, age_range)

        groupings = {
            AGE: dict(age_groupings)
        }
        return name, groupings


    def splitage(self, ages, name=None):
        """
        spans the entire AGE dimension and groups ages into bins based on
        which ages are specified

        Inputs:
            ages (list): a list of integers that indicate the break/split points

        Outputs:
            a name and dictionary for building a recoded variable in the schema

        Notes:
            The break/split points indicate the right boundary of each group, with
            the last group being automatically calculated

            Example:
                splitage(17) implies two groups:
                    A. 0 to 17
                    B. 18 to 116

                splitage([17, 64]) implies three groups:
                    A. 0 to 17
                    B. 18 to 64
                    C. 65 to 116
        """
        ages = das_utils.aslist(ages)

        if name is None:
            name = f"split{'and'.join([str(x) for x in ages])}years"

        agedict = self.getLevels()[AGE]
        maxage = max(agedict.values())[0]
        agearr = np.array(ages)
        assert np.all(agearr >= 0) and np.all(agearr <= maxage-1), f"'ages' should be between 0 and {maxage-1}"
        ages = [0] + ages + [maxage]
        age_pairs = [(ages[i], ages[i+1]) for i in range(len(ages)-1)]

        age_groupings = []
        for i,pair in enumerate(age_pairs):
            print(f"{pair[0]}   {pair[1]}")
            if i == 0:
                if pair[0] == pair[1]:
                    if pair[0] == 0:
                        label = f"Under 1 year"
                    elif pair[0] == 1:
                        label = f"1 year"
                    else:
                        label = f"{pair[0]+1} years"
                else:
                    label = f"Under {pair[1]+1} years"

                level = list(range(pair[0], pair[1]+1))

            else:
                if i == len(age_pairs)-1:
                    if pair[0] == 0:
                        label = f"{pair[0]+1} year and over"
                    else:
                        label = f"{pair[0]+1} years and over"
                elif pair[0] == pair[1]-1:
                    label = f"{pair[0]+1} years"
                else:
                    label = f"{pair[0]+1} to {pair[1]} years"

                level = list(range(pair[0]+1, pair[1]+1))

            age_groupings.append( (label, level) )

        groupings = {
            AGE: dict(age_groupings)
        }
        return name, groupings

