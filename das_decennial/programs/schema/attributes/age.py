from programs.schema.attributes.abstractattribute import AbstractAttribute
from constants import CC


class AgeAttr(AbstractAttribute):

    @staticmethod
    def getName():
        return CC.ATTR_AGE

    @staticmethod
    def getLevels():
        return  {
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
            }


    @staticmethod
    def recodeAgecat43():
        """
        for table P43, recode the age variable to reflect the following age groups
            Under 18
            18 to 64
            65 and over
        """
        name = CC.AGE_CAT43

        groups = {
                "Under 18 years"   : list(range(0,18)),
                "18 to 64 years"   : list(range(18,65)),
                "65 years and over": list(range(65,116))
            }
        return name, groups

    @staticmethod
    def recodeAgroup4():
        """ Age groups in ranges of 4, part of HB Tree Workload with fanout of 4 on 116 age values """
        name = CC.AGE_GROUP4
        groups = {
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
        return name, groups

    @staticmethod
    def recodeAgegroup16():
        """ Age groups in ranges of 16, part of HB Tree Workload with fanout of 4 on 116 age values """
        name = CC.AGE_GROUP16
        groups = {
                "Under 16 years" : list(range(0, 16)),
                "16 to 31 years" : list(range(16, 32)),
                "32 to 47 years": list(range(32, 48)),
                "48 to 63 years": list(range(48, 64)),
                "64 to 79 years": list(range(64, 80)),
                "80 to 95 years": list(range(80, 96)),
                "96 to 111 years": list(range(96, 112)),
                "112 to 115 years": list(range(112, 116)),
            }
        return name, groups

    @staticmethod
    def recodeAgegroup64():
        """ Age groups in ranges of 64, part of HB Tree Workload with fanout of 4 on 116 age values """
        name = CC.AGE_GROUP64
        groups = {
                "Under 64 years" : list(range(0, 64)),
                "64 to 115 years" : list(range(64, 116)),
            }
        return name, groups

    @staticmethod
    def recodeVotingLevels():
        """
        Groups the AGE variable into
            Non voting age persons
            Voting age persons
        """
        name = CC.AGE_VOTINGLEVELS
        groups = {
                "Non-voting Age": list(range(18)),
                "Voting Age"    : list(range(18,116))
            }
        return name, groups

    @staticmethod
    def recodeNonvotingTotal():
        """
        Groups the AGE variable into
            Non voting age persons
            Voting age persons
        """
        name = CC.AGE_NONVOTING_TOTAL
        groups = {
                "Non-voting Age": list(range(18)),
            }
        return name, groups

    @staticmethod
    def recodeVotingTotal():
        """
        Groups the AGE variable into
            Non voting age persons
            Voting age persons
        """
        name = CC.AGE_VOTING_TOTAL
        groups = {
                "Voting Age": list(range(18, 116)),
            }
        return name, groups


    @staticmethod
    def recodeUnder15():
        name = CC.AGE_UNDER15_TOTAL
        groups = {
                'Under 15': list(range(15))
            }
        return name, groups

    #From DHCP_HHGQ
    @staticmethod
    def recodePrefix_agecats():
        """
        Create the hierarchical age categories requested by Dan (8/8). These are for the 'prefix' queries.
        """
        name = CC.AGE_PREFIX_AGECATS
        agedict = {}
        for j in range(116):
            k = j + 1
            if j == 0:
                label = f"age {j}"
            else:
                label = f"age {0}-{j}"

            agedict.update({label: list(range(0, k))})

        groupings = agedict
        return name, groupings

    @staticmethod
    def recodeRange_agecats():
        """
        Create the hierarchical age categories requested by Phil (8/8). These are for the 'range' queries.
        """
        name = CC.AGE_RANGE_AGECATS
        agedict = {}
        for j in range(116):
            for k in range((j + 1)):
                if j == k:
                    label = f"age {k}"
                else:
                    label = f"age {k}-{j}"

                agedict.update({label: list(range(k, j + 1))})

        groupings = agedict
        return name, groupings

    @staticmethod
    def recodeBinarysplit_agecats(maxage=115, splitlist=[116, 58, 29, 15, 7, 4, 2, 1]):
        """
        Create the hierarchical age categories requested initially by Phil (8/7). These are for the 'binary split' queries.
        This buckets ages into equally sized* age groups based on the size of the age groups given in splitlist.
        The last age category will be truncated to the maximum age, given by maxage, in cases where the age range cannot be broken
        into equally sized age groups.

        Arguments:  maxage takes the maximum age in data range, 115 as of Aug. 2019.
                    splitlist takes a list of integers, where the integers indicate the number of ages to include per category.

        """
        name = CC.AGE_BINARYSPLIT_AGECATS
        agedict = {}
        for split in splitlist:
            start = 0
            while start < maxage:
                agelist = list(range(start, start + split))
                agelist = [age for age in agelist if age <= maxage]  # For last category, remove excess ages since they may go above maxage
                end = agelist[-1]
                if start == end:
                    label = f"split{split}: age {start}"
                else:
                    label = f"split{split}: age {start}-{end}"
                agedict.update({label: agelist})
                start = end + 1

        groupings = agedict
        return name, groupings

    @staticmethod
    def recodeAge_lessthan15():
        name = CC.AGE_LT15_TOTAL
        groupings =  {"Age is <15": list(range(0, 15))}
        return name, groupings

    @staticmethod
    def recodeAge_greaterthan25():
        name = CC.AGE_GT25_TOTAL
        groupings = {"Age is >25": list(range(26, 116))}
        return name, groupings

    @staticmethod
    def recodeAge_lessthan20():
        name = CC.AGE_LT20_TOTAL
        groupings = {"Age is <20": list(range(0, 20))}
        return name, groupings

    @staticmethod
    def recodeAge_lt17gt65():
        name = CC.AGE_LT17GT65_TOTAL
        groupings = {"Age is <17 or >65": list(range(0, 17)) + list(range(66, 116))}
        return name, groupings

    @staticmethod
    def recodeAge_lt16gt65():
        name = CC.AGE_LT16GT65_TOTAL
        groupings = {"Age is <16 or >65 ": list(range(0, 16)) + list(range(66, 116))}
        return name, groupings

    @staticmethod
    def recodeAgecatPCO3():
        """
        Groups AGE into 5 year bands above age 20
        """
        name = CC.AGE_PC03_LEVELS
        groupings = {
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
        return name, groupings

    @staticmethod
    def recodeAgecatPCO4():
        """
        Groups AGE into 5 year bands below age 25
        """
        name = CC.AGE_PC04_LEVELS
        groupings = {'Under 5 years': list(range(0, 5)), '5 to 9 years': list(range(5, 10)), '10 to 14 years': list(range(10, 15)),
            '15 to 19 years': list(range(15, 20)), '20 to 24 years': list(range(20, 25)), '25 years and over': list(range(25, 116))}
        return name, groupings

