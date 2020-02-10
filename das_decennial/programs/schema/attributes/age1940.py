from programs.schema.attributes.age import AgeAttr
from constants import CC


class Age1940Attr(AgeAttr):
    @staticmethod
    def getName():
        return CC.ATTR_AGE_1940

    @staticmethod
    def recodeAgeGroups4():
        """ Age groups in ranges of 4, part of HB Tree Workload with fanout of 4 on 116 age values """
        name = "ageGroups4"
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
    def recodeAgeGroups16():
        """ Age groups in ranges of 16, part of HB Tree Workload with fanout of 4 on 116 age values """
        name = "ageGroups16"
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
    def recodeAgeGroups64():
        """ Age groups in ranges of 64, part of HB Tree Workload with fanout of 4 on 116 age values """
        name = "ageGroups64"
        groups = {
            "Under 64 years" : list(range(0, 64)),
            "64 to 115 years" : list(range(64, 116)),
        }
        return name, groups
