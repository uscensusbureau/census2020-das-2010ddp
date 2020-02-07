from programs.writer.mdf2020writer import MDF2020PersonWriter
from programs.writer.dhcp_hhgq_to_mdfpersons2020 import DHCPHHGQToMDFPersons2020Recoder

# Schema / Node columns

from constants import CC

HHGQ = CC.ATTR_HHGQ_1940
SEX = CC.ATTR_SEX_1940
AGE = CC.ATTR_AGE_1940
HISP = CC.ATTR_HISPANIC_1940
RACE_1940 = CC.ATTR_RACE_1940
CITIZEN = CC.ATTR_CITIZEN_1940


class IPUMS1940ToMDFPersons2020Recoder(DHCPHHGQToMDFPersons2020Recoder):
    """
    Differences between IPUMS 1940 and DDP:
    Tabblkcou is length 4 in 1940s
    Enumdist included (Char(4))
    Supdist included (Char(3))
    """

    schema_name = CC.DAS_1940

    def recode(self, row, nullfill=False):
        """
        mapper for Row objects in spark dataframe
        recodes variables from 1940s DDP DHC-P histogram spec
        to the MDF 2020 person spec
        Inputs:
            row: dict - a dictionary representation of the Row object
        """
        uncond_recodes = [
            self.schema_type_code_recoder,
            self.schema_build_id_recoder,
            self.tabblkst_recoder,
            self.tabblkcou_recoder,
            self.enumdist_recoder,
            self.supdist_recoder,
            self.epnum_recoder, # Not reported
            self.rtype_recoder,
            self.gqtype_recoder, # was not in E2E
            self.relship_recoder, # not reported
            self.qsex_recoder,
            self.qage_recoder,
            self.cenhisp_recoder,
            self.cenrace_recoder,
            # self.citizen_recoder, # Not included in DDP output
            self.live_alone_recoder
        ]

        empty_var_list = [
        ]

        for recode in uncond_recodes:
            row = recode(row)

        for var in empty_var_list:
            row[var] = "9"

        for varname in [self.geocode] + self.mangled_dimnames:
            row.pop(varname, None)

        return row

    def tabblkcou_recoder(self, row):
        """
        adds the TABBLKCOU column to the row
        refers to the 2020 Tabulation County (FIPS)
        CHAR(4) in the 1940s schema. See
        row: dict
        """
        row['TABBLKCOU'] = row[self.geocode][2:6]
        return row

    def enumdist_recoder(self, row):
        """
        adds the ENUMDIST column to the row
        refers to the 2020 Tabulation State (FIPS)
        CHAR(4)
        row: dict
        """
        row['ENUMDIST'] = row[self.geocode][9:13]
        return row

    def supdist_recoder(self, row):
        """
        adds the SUPDIST column to the row
        CHAR(3)
        row: dict
        """
        row['SUPDIST'] = row[self.geocode][6:9]
        return row

    def qage_recoder(self, row):
        """
        adds the QAGE column to the row
        refers to the Edited Age
        INT(3)
        row: dict
        """
        age = int(row[self.getMangledName(AGE)])
        row['QAGE'] = age
        return row

    def cenrace_recoder(self, row):
        """
        adds the CENRACE column to the row
        refers to the CensusRace
        CHAR(2)
        row: dict
        Notes:
            Offset from the histogram encoding. (e.g. "01" = White alone = cenrace[0])
        """
        cenrace = int(row[self.getMangledName(RACE_1940)])
        # add offset
        cenrace = cenrace + 1
        if cenrace < 10: # add 0 padding to single-digit codes
            cenrace = f"0{cenrace}"
        else:
            cenrace = str(cenrace)
        row['CENRACE'] = cenrace
        return row

    def cenhisp_recoder(self, row):
        """
        adds the CENHISP column to the row
        refers to the Hispanic Origin
        CHAR(1)
        row: dict
        """
        hispanic = int(row[self.getMangledName(HISP)])
        _NOT_HISPANIC = "1"
        _HISPANIC = "2"
        if hispanic in [0]:
            cenhisp = _NOT_HISPANIC
        else:
            cenhisp = _HISPANIC
        row['CENHISP'] = str(cenhisp)
        return row

    def rtype_recoder(self, row):
        """
        adds the RTYPE column to the row
        refers to the Record Type
        CHAR(1)
        row: dict
            Notes: In the histogram, hhgq[0] refers to housing unit; hhgq[1:8] refers to group quarter types
        """
        hhgq = int(row[self.getMangledName(HHGQ)])
        _HOUSING_UNIT = "3"
        _GROUP_QUARTERS = "5"
        if hhgq in [0]:
            rtype = _HOUSING_UNIT
        else:
            rtype = _GROUP_QUARTERS
        row['RTYPE'] = str(rtype)
        return row

    def gqtype_recoder(self, row):
        """
        adds the GQTYPE column to the row
        refers to the Group Quarters Type
        CHAR(3)
        row: dict
        Notes:
            Since the HHGQ variable only has major group quarters information,
            the recodes group detailed categories into major GQ bands.
        """
        hhgq = int(row[self.getMangledName(HHGQ)])
        if hhgq in [0]:
            gqtype = "000"     # NIU (i.e. household)
        elif hhgq in [1]:
            gqtype = "101"     # 101 - 106
        elif hhgq in [2]:
            gqtype = "201"     # 201 - 203
        elif hhgq in [3]:
            gqtype = "301"     # 301
        elif hhgq in [4]:
            gqtype = "401"     # 401 - 405
        elif hhgq in [5]:
            gqtype = "501"     # 501 - 502
        elif hhgq in [6]:
            gqtype = "601"     # 601 - 602
        else:
            gqtype = "701"     # 701 - 706; 801 - 802; 900 - 904
        row['GQTYPE'] = str(gqtype)
        return row

    def citizen_recoder(self, row):
        """
        adds the CITIZEN column to the row
        refers to the Edited Citizenship
        CHAR(1)
        row: dict
        """
        cit = int(row[self.getMangledName(CITIZEN)])
        _CITIZEN = "1"
        _NON_CITIZEN = "2"
        if cit in [1]:
            citizen = _CITIZEN
        else:
            citizen = _NON_CITIZEN
        row['CITIZEN'] = str(citizen)
        return row

    def qsex_recoder(self, row):
        """
        adds the QSEX column to the row
        refers to the Edited Sex
        CHAR(1)
        row: dict
        """
        sex = int(row[self.getMangledName(SEX)])
        _MALE = "1"
        _FEMALE = "2"
        if sex in [0]:
            qsex = _MALE
        else:
            qsex = _FEMALE
        row['QSEX'] = str(qsex)
        return row


class IPUMSPersonWriter(MDF2020PersonWriter):
    row_recoder = IPUMS1940ToMDFPersons2020Recoder

    var_list = [
        "SCHEMA_TYPE_CODE",
        "SCHEMA_BUILD_ID",
        "TABBLKST",
        "TABBLKCOU",
        # "TABTRACTCE", out for 1940
        # "TABBLKGRPCE", out for 1940
        # "TABBLK", out for 1940
        "SUPDIST",  # special for 1940
        "ENUMDIST",  # special for 1940
        "EPNUM",
        "RTYPE",
        "GQTYPE",
        "RELSHIP",
        "QSEX",
        "QAGE",
        "CENHISP",
        "CENRACE",
        # "CITIZEN",
        "LIVE_ALONE"
    ]

