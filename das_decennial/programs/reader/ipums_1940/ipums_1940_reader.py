""" Module docstring """

from programs.reader.fwf_reader import FixedWidthFormatTable

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import Row
    from pyspark.sql.functions import concat, col
except ImportError as e:
    pass

class person_recoder:
    """
        This is the recoder for the IPUMS 1940s data for the Updated DHCP Schema.
        It creates hhgq1940, sex1940, age1940, hispan1940, and citizen1940 variables.
    """
    def __init__(self, gq_varname_list, sex_varname_list, age_varname_list, hispan_varname_list,
                 race_varname_list, citizen_varname_list):
        self.gq_varnames = gq_varname_list
        self.sex = sex_varname_list[0]
        self.age = age_varname_list[0]
        self.hispan = hispan_varname_list[0]
        self.race = race_varname_list[0]
        self.citizen = citizen_varname_list[0]

    def recode(self, row):
        """
            Input:
                row: original dataframe Row

            Output:
                dataframe Row with recode variables added
        """
        # Get the values for all the variables needed for recoding
        gq = int(row[self.gq_varnames[0]])
        gqtype = int(row[self.gq_varnames[1]])
        sex = int(row[self.sex])
        age = int(row[self.age])
        hispan = int(row[self.hispan])
        race = int(row[self.race])
        citizen = int(row[self.citizen])

        # HHGQ for 1940s is recoded from the GQ and GQTYPE (obtained prior by joining with the unit table)
        hhgq1940 = map_to_hhgq(gq, gqtype)

        # 1940s SEX has 2 values, which are re-indexed to 0 and 1 by subtracting 1, to match the 1940 DHCP Schema
        assert sex in [1, 2], f'Incorrect sex in input data for row: {str(row)}'
        sex1940 = sex-1

        # 1940s AGE has a maximum of 120. Top code at 115 to match the 1940 DHCP Schema
        assert 0 <= age <= 120, f'Incorrect age in input data for row: {str(row)}'
        age1940 = 115 if age > 115 else age

        # 1940s HISPAN has 5 values. Map Not Hispanic (0) to Not Hispanic (0), and all others to Hispanic (1)
        # to match the 1940 DHCP Schema
        assert hispan in [0, 1, 2, 3, 4], f'Incorrect hispan in input data for row: {str(row)}'
        hispanic1940 = 0 if hispan == 0 else 1

        # 1940s RACE has 6 values, which are re-indexed by subtracting 1, to match the 1940 DHCP Schema
        assert race in [1, 2, 3, 4, 5, 6], f'Incorrect hispan in input data for row: {str(row)}'
        cenrace1940 = race

        # 1940s CITIZEN has 5 values, recoded as follows to match the 1940 DHCP Schema:
        # N/A                                          (0) -> Citizen (1)
        # Born abroad of American parents              (1) -> Citizen (1)
        # Naturalized citizen                          (2) -> Citizen (1)
        # Not a citizen                                (3) -> Not a Citizen (0)
        # Not a citizen, but has received first papers (4) -> Not a Citizen (0)
        assert hispan in [0, 1, 2, 3, 4], f'Incorrect hispan in input data for row: {str(row)}'
        citizen1940 = 1 if citizen in [0, 1, 2] else 0

        row = Row(**row.asDict(), hhgq1940=hhgq1940, sex1940=sex1940, age1940=age1940, hispanic1940=hispanic1940,
                  cenrace1940=cenrace1940, citizen1940=citizen1940)
        return row


class Persons1940Table(FixedWidthFormatTable):
    def load(self, filter=None):
        self.annotate(str(self.location))

        # Store the unit table as a local variable instead of class variable to avoid an issue
        # during serialization when calling a UDF in an RDD's map function
        # Better factoring of table_reader's load method to include a postprocessing step would remove this requirement
        uwc = self.unit_with_geocode
        self.unit_with_geocode = None

        person_df = super().load(filter=lambda line: line[0] == 'P')

        # Join (on serial) with the subset of the unit table, which includes geocode, gq, and gqtype,
        # which are necessary for correctly recoding into the DHCP schema variables
        final_df = person_df.join(uwc, on=['serial'], how='inner')

        return final_df


class Household1940Table(FixedWidthFormatTable):
    def load(self, filter=None):
        self.annotate(str(self.location))
        unit_df = super().load(filter=lambda line: line[0] == 'H')

        unit_with_geocode = unit_df.withColumn('geocode', concat(col('statefip'), col('county'), col('supdist'),
                                                                    col('enumdist'))).select(['serial', 'geocode', 'gq', 'gqtype'])
        self.reader.tables['PersonData'].unit_with_geocode = unit_with_geocode
        return unit_df



class unit_recoder:
    """
        This is a minimal recoder for reading in the Households in IPUMS 1940 Census dataset.
        It recodes for the minimal schema necessary to allow the DHC-P schema recodes to function.
        It creates hhgq1940 and geocode variables.
    """
    def __init__(self, gq_varname, geo_vars):
        self.gq = gq_varname
        self.geocode = geo_vars

    def recode(self, row):
        """
            Input:
                row: an original dataframe Row

            Output:
                a dataframe Row with recode variables added
        """
        hhgq1940 = map_to_hhgq(int(row[self.gq[0]]), int(row[self.gq[1]]))
        geocode = geocode_recode(row, self.geocode)
        row = Row(**row.asDict(), hhgq1940=hhgq1940, geocode=geocode)
        return row


def map_to_hhgq(gq, gqtype):
    """
        args:
            gqtype - 3 char str encoding gqtype
        returns: hhgq for 2018 tab of P42.
    """
    # For the 1940 data, gqtypes 1,5 do not appear. Only 0,2,3,4,6,7,8,9. Values are shifted down to 0-7.

    #  1970 + "additional HH under 1990" + "additional HH under 2000" to define households (GQ=1).
    hhgq = -1

    if gqtype == 0:
        hhgq = 0
    # gqtype of 1 doesn't exist in the 1940s data
    elif gqtype in [2, 3, 4]:
        hhgq = gqtype - 1
    # gqtype of 5 doesn't exist in the 1940s data
    elif gqtype in [6, 7, 8]:
        hhgq = gqtype - 2
    elif gqtype == 9:
        # The GQTYPE was set to 9 ("Other non-institutional GQ and unknown") for too many units in the 1940s data
        # We want to define the following GQ types as Not a Group Quarters as well:
        #   GQ == 1 (Household under 1940 Definition)
        #   GQ == 2 (Additional households under 1940 definition)
        #   GQ == 3 (Other Group Quarters)
        if gq in [1, 2, 5]:
            hhgq = 0
        # All others can stay as "Other non-institutional GQ and unknown"
        else:
            hhgq = 7

    assert hhgq >= 0, f'Could not recode hhgq for values gq: {gq}, gqtype:{gqtype}'
    return hhgq


def geocode_recode(row, geocode_vars):
    """
        This function creates the geocode variable from 4 configured variables.

        Inputs:
            row: a SQL Row
            geocode_vars: the names of the 4 geocode variable to be concatenated to produce geocode

        Output:
            a new row with geocode added
    """

    return str(row[geocode_vars[0]]) + str(row[geocode_vars[1]]) + str(row[geocode_vars[2]] + str(row[geocode_vars[3]]))
