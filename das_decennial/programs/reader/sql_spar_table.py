"""
    This is a class that reads table and converts it from Spark DataFrame with rows corresponding to records
    (person records, or household records, or housing unit records) into RDD with histograms (as sparse matrices)
    keyed by geocode, using Spark SQL up until creation of scipy.sparse dok matrices representing the histograms
"""

import numpy as np
import scipy.sparse as ss
import pyspark.sql

import programs.reader.spar_table
from programs.das_rdd import timeit, DASDataFrame


@timeit
def runSQLQuery(spark, query):
    return DASDataFrame(spark.sql(query))


class SQLSparseHistogramTable(programs.reader.spar_table.SparseHistogramTable):
    """
    This is a class that reads table and converts it from Spark DataFrame with rows corresponding to records
    (person records, or household records, or housing unit records) into RDD with histograms (as sparse matrices)
    keyed by geocode, using Spark SQL up until creation of scipy.sparse dok matrices representing the histograms
    """

    HISTDIC = 'histdic'
    TABLENAME = 'table'
    COUNTVAR = 'count'

    @staticmethod
    def nprmi(idx, shape):
        """
        This function only exists because it is easily unit tested, to serve as a template to nprmiSQLString.
        It does exactly the same thing as numpy.ravel_multi_index
        """
        s = 0
        for v, dim in zip(idx, np.array(list(shape) + [1])[1:]):
            s = (s + v) * dim
        return s

    def nprmiSQLString(self):
        """
        Make part of SQL query which converts histogram indices to 1D index for CSR matrix,
        in the same way as numpy.ravel_multi_index makes 1D from multiD index
        """
        s = "0"
        for v, dim in zip(self.histogram_variables, np.array(list(self.data_shape) + [1])[1:]):
            s = f"(({s} + {v}) * {dim})"
        return s

    def make_spar(self, row: pyspark.sql.Row):
        geoid = tuple(row[v] for v in self.geography_variables)
        size = np.prod(self.data_shape)
        spar = ss.dok_matrix((1, size), dtype=int)
        for d in row[self.HISTDIC]:
            spar[0, d[0]] = d[1]
        return geoid, spar.tocsr()

    def process(self, data: pyspark.sql.dataframe.DataFrame):
        """
        args:
            a Spark DataFrame containing CEF person, household, or unit records

        This function performs the following process:
            (1) Count records, grouping by all variables used (i.e. histogram variables + geocode). This is done in SQL sub-query
                Now we have a data frame showing how many records are there for each geocode and each histogram variable combination
            (2) Convert each histogram variables combination into a 1D index (using self.nprmiSQLString function, which forms the corresponding part
                of the SQL query) and
            (3) Make array using that index, with value being the count of records corresponding to the histogram variables combination yielding that
                index (the count is calculated in step 1) and
            (4) Collect all those arrays into a list over all histogram variables combination, keeping grouping by geocode
                (The result is analogous to having a python dictionary for each geocode, with keys being 1D index (corresponding to a particular
                histogram variables combination) an value being the count. This "dict" only contains non-zero values of the sparse histogram and
                thus is smaller than full array representation)
            (5) Out of each of those lists (~python dicts) make a sparse matrix (ss.csr_matrix) with self.make_spar function

        Points (1)-(4) are achieved with a single Spark SQL query, to which (1) is a sub-query, i.e. (1) forms a table with counts,
        and then a single SELECT query performs (2),(3) and (4), selecting from the table made in (1)

        returns: an rdd of (geocode, scipy.sparcse.csr_matrix) pairs.

        Example (from unit test):
            If we have 4 records:
                   g   a  b
                 ("1", 1, 0),
                 ("1", 1, 0),
                 ("1", 1, 1),
                 ("2", 1, 1)
            where "g" is geocode (or 1 geography variable) and "a" and "b" are histogram variables, then
            (1) count:
                  g="1", a=1, b=0: count=2
                  g="1", a=1, b=1: count=1
                  g="2", a=1, b=1: count=1
            (2) 1D index (like numpy.ravel_multi_index). If a and b can each be 0 or 1, then the dimension of the histogram is (2,2), and the
                  indices are converted like (0,0)->0, (0,1)->1, (1,0)->2, (1,1)->3. So we have
                  g="1", index=2, count=2
                  g="1", index=3, count=1
                  g="2", index=3, count=1
            (3) Make array from index and count (i.e. index is 1st element, count is second)
                  g="1", ind_cnt_array=[2,2]
                  g="1", ind_cnt_array=[3,1]
                  g="2", ind_cnt_array=[3,1]
                we can write inc_cnt_array combination as a dict/map entry:
                  g="1", hist_dict_entry=2->2
                  g="1", hist_dict_entry=3->1
                  g="2", hist_dict_entry=3->1
            (4) Collect over all indices / "a"-"b"-combinations, keeping the "g" grouping:
                  g="1", histdic=2->2, 3->1
                  g="2", histdic=3->1
                (Using -> notation for higher transparency of the purpose, but really within Spark DataFrame these are just lists of arrays:
                  g="1", histdic=[2,2], [3,1]
                  g="2", histdic=[3,1]
                  Uniqueness of the histdic "key", which is really just the first array element and not a key is guaranteed by the ravel_multi_index
                  transformation, i.e. that each (a,b) combination corresponds to one and only one integer)

            (5) So for g="1" we will have a sparse matrix of length 4, with 2 non-zeros: at index 2 and 3; and for g="2" we have a sparse matrix of length 4
                with 1 non-zero at index 3. The values (i.e. histogram counts) are 2, 1 and 1 respectively.
        """

        data = self.repartitionData(data)
        print("Converting to histogram")

        data.createOrReplaceTempView(self.TABLENAME)
        spark = pyspark.sql.SparkSession.builder.getOrCreate()
        allvars = ",".join(self.geography_variables + self.histogram_variables)
        gv = ",".join(self.geography_variables)
        # (2,3,4) Make array/list from hist vars and the count from (1) as [1d_index_from hist_vars, count] and collect them over hist vars, leaving geo vars
        # (1) Count all people by geo, by hist
        # the grouping by geo is for (2)
        query = f"SELECT {gv}, collect_list(array({self.nprmiSQLString()}, {self.COUNTVAR})) AS {self.HISTDIC} FROM " \
                    f"(SELECT {allvars}, COUNT(1) AS {self.COUNTVAR} FROM {self.TABLENAME} GROUP BY {allvars}) " \
                f"GROUP BY {gv}"
        # If measuring times on DataFrame and RDD operations, convert the DataFrame to DASDataFrame and time it.
        df = spark.sql(query) if not self.reader.measure_rdd_times else runSQLQuery(spark, query)
        # Make RDD of sparse matrices keyed by geographic variables
        df = df.rdd.map(self.make_spar)
        return df
