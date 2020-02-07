from programs.nodes.nodes import GEOCODE
from programs.schema.schemas.schemamaker import SchemaMaker

class MDF2020Recoder:

    schema_name = None

    def __init__(self):
        self.schema = SchemaMaker.fromName(self.schema_name)

        self.mangled_dimnames = list(map(lambda name: name + f"_{self.schema.name}", self.schema.dimnames))

        self.geocode = GEOCODE

    def niuFiller(self,  length, type='str'):
        if type == 'int':
            return 0
        elif type == 'str':
            return '0' * length
        else:
            raise ValueError("Only filling int or str")

    def notSupprortedFiller(self,  length, type='str'):
        if type == 'int':
            return int(10 ** length - 1)
        elif type == 'str':
            return '9' * length
        else:
            raise ValueError("Only filling int or str")

    def getMangledName(self, name):
        return self.mangled_dimnames[self.schema.dimnames.index(name)]

    @staticmethod
    def schema_build_id_recoder(row):
        """
        adds the SCHEMA_BUILD_ID column to the row
        CHAR(5)
        row: dict
        """
        row['SCHEMA_BUILD_ID'] = "1.1.0"
        return row

    def tabblkst_recoder(self, row):
        """
        adds the TABBLKST column to the row
        refers to the 2020 Tabulation State (FIPS)
        CHAR(2)
        row: dict
        """
        row['TABBLKST'] = row[self.geocode][0:2]
        return row

    def tabblkcou_recoder(self, row):
        """
        adds the TABBLKCOU column to the row
        refers to the 2020 Tabulation County (FIPS)
        CHAR(3)
        row: dict
        """
        row['TABBLKCOU'] = row[self.geocode][2:5]
        return row

    def tabtractce_recoder(self, row):
        """
        adds the TABTRACTCE column to the row
        refers to the 2020 Tabulation Census Tract
        CHAR(6)
        row: dict
        """
        row['TABTRACTCE'] = row[self.geocode][5:11]
        return row

    def tabblkgrpce_recoder(self, row):
        """
        adds the TABBLKGRPCE column to the row
        refers to the 2020 Census Block Group
        CHAR(1)
        row: dict
        """
        row['TABBLKGRPCE'] = row[self.geocode][11:12]
        return row

    def tabblk_recoder(self, row):
        """
        adds the TABBLK column to the row
        refers to the 2020 Block Number
        CHAR(4)
        row: dict
        """
        row['TABBLK'] = row[self.geocode][12:16]
        return row
