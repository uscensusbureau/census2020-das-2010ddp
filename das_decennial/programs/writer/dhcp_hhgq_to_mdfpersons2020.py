# import constants as con
from programs.writer.mdf2020_recoder import MDF2020Recoder

# Schema / Node columns

from constants import CC

HHGQ = CC.ATTR_HHGQ
SEX = CC.ATTR_SEX
AGE = CC.ATTR_AGE
HISP = CC.ATTR_HISP
CENRACE = CC.ATTR_CENRACE
CITIZEN = CC.ATTR_CITIZEN


class DHCPHHGQToMDFPersons2020Recoder(MDF2020Recoder):

    # schema_name = CC.DAS_DHCP_HHGQ

    def recode(self, row, nullfill=False):
        """
        mapper for Row objects in spark dataframe
        recodes variables from hh2010 histogram spec
        to the requested mdf 2020 unit spec
        Inputs:
            row: dict - a dictionary representation of the Row object
        """
        uncond_recodes = [
            self.schema_type_code_recoder,
            self.schema_build_id_recoder,
            self.tabblkst_recoder,
            self.tabblkcou_recoder,
            self.tabtractce_recoder,
            self.tabblkgrpce_recoder,
            self.tabblk_recoder,

            self.epnum_recoder,
            self.rtype_recoder,
            self.gqtype_recoder,
            self.relship_recoder,
            self.qsex_recoder,
            self.qage_recoder,
            self.cenhisp_recoder,
            self.cenrace_recoder,
            self.citizen_recoder,
            self.live_alone_recoder
        ]

        niu_fill_recodes = [
        ]

        not_supported_fill_recodes = [
        ]

        for recode in uncond_recodes:
            row = recode(row)

        if nullfill:
            for recode in niu_fill_recodes:
                row = recode(row, nullfiller=self.niuFiller)

            for recode in not_supported_fill_recodes:
                row = recode(row, nullfiller=self.notSupportedFiller)
        else:
            for recode in niu_fill_recodes + not_supported_fill_recodes:
                row = recode(row, nullfiller=None)

        for varname in [self.geocode] + self.mangled_dimnames:
            row.pop(varname, None)

        return row

    @staticmethod
    def schema_type_code_recoder(row):
        """
        adds the SCHEMA_TYPE_CODE column to the row
        CHAR(3)
        row: dict
        """
        row['SCHEMA_TYPE_CODE'] = "MPD"
        return row

    @staticmethod
    def epnum_recoder(row):
        """
        adds the EPNUM column to the row
        refers to the Privacy Edited Person Number
        INT(9)
        row: dict
        Notes:
            In MDF spec, values say they can range from 0-99999, but based
            on the INT(9) value, they should range from 0-999999999.
            Not currently supported, so all values will be 999999999.
        """
        _NOT_SUPPORTED = 999999999
        row['EPNUM'] = int(_NOT_SUPPORTED)
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

    def relship_recoder(self, row):
        """
        adds the RELSHIP column to the row
        refers to the Edited Relationship
        CHAR(2)
        row: dict
        Notes:
            All relationship to householder levels have been collapsed
            into a single level of the hhgq variable (hhgq[0]), so this
            variable is not currently supported.
            As such, all values will be "99" to match the CHAR(2) requirement.
        """
        _NOT_SUPPORTED = "99"
        row['RELSHIP'] = str(_NOT_SUPPORTED)
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

    def qage_recoder(self, row):
        """
        adds the QAGE column to the row
        refers to the Edited Age
        INT(3)
        row: dict
        """
        age = int(row[self.getMangledName(AGE)])
        row['QAGE'] = int(age)
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

    def cenrace_recoder(self, row):
        """
        adds the CENRACE column to the row
        refers to the CensusRace
        CHAR(2)
        row: dict
        Notes:
            Offset from the histogram encoding. (e.g. "01" = White alone = cenrace[0])
        """
        cenrace = int(row[self.getMangledName(CENRACE)])
        # add offset
        cenrace = cenrace + 1
        if cenrace < 10: # add 0 padding to single-digit codes
            cenrace = f"0{cenrace}"
        else:
            cenrace = str(cenrace)
        row['CENRACE'] = cenrace
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

    def live_alone_recoder(self, row):
        """
        adds the LIVE_ALONE column to the row
        refers to the Person Living Alone
        CHAR(1)
        row: dict
        Notes:
            Not currently supported, so it's filled with "9".
        """
        _NOT_SUPPORTED = "9"
        row['LIVE_ALONE'] = str(_NOT_SUPPORTED)
        return row

