"""
This module implements a few table reading classes.

TableWithGeocode is a class that has a repartitionData function, that performs repartitioning of DataFrame by geocode before
creating histograms.

SparseHistogramTable is a class that reads table and converts it from Spark DataFrame with rows corresponding to records
(person records, or household records, or housing unit records) into RDD with histograms (as sparse matrices)
keyed by geocode, using python mapping functions.
"""

from operator import add
import numpy as np
import scipy.sparse as ss
from pyspark.sql.functions import substring

from programs.reader.table import DenseHistogramTable, UnitFromPersonTable


class TableWithGeocode(DenseHistogramTable):
    """
    Class to implement common methods between person and unit tables or others with properties that
    are common, but not common enough to put into AbstractTable
    """
    PARTITION_CODE_COLUMN = 'partitionCode'

    def repartitionData(self, data):
        """
        Perform table repartitioning in spark depending on settings in config
        :param data:
        :return:
        """
        print(f"{self.__class__.__name__} received microdata dataframe with number of partitions: {data.rdd.getNumPartitions()}")
        print(f"& schema: {data.printSchema()}")
        print(f"{self.__class__.__name__} is repartitioning microdata dataframe to {self.reader.num_reader_partitions} partitions, ")
        print("collapsing to a sparse histogram representation, and passing back to driver...")
        if self.reader.num_reader_partitions > 0:
            data = data.withColumn(self.PARTITION_CODE_COLUMN, substring(self.geography_variables[0], 0, self.reader.reader_partition_len))
            if not self.reader.range_partition:
                print(f"Using df hash partitioner by {self.PARTITION_CODE_COLUMN}")
                return data.repartition(self.reader.num_reader_partitions, self.PARTITION_CODE_COLUMN).drop(self.PARTITION_CODE_COLUMN)

            print(f"Using df range partitioner by {self.PARTITION_CODE_COLUMN}")
            return data.repartitionByRange(self.reader.num_reader_partitions, self.PARTITION_CODE_COLUMN).drop(self.PARTITION_CODE_COLUMN)
        return data


class SparseHistogramTable(TableWithGeocode):
    """
    This is a class that reads table and converts it from Spark DataFrame with rows corresponding to records
    (person records, or household records, or housing unit records) into RDD with histograms (as sparse matrices)
    keyed by geocode, using python mapping functions.
    """

    def to_by_geo(self, pair):

        # Would prefer using dok_matrix but this bug isn't fixed in our version.
        # https://github.com/scipy/scipy/issues/7699

        blk_idx, val = pair
        blk = blk_idx[:len(self.geography_variables)]
        idx = blk_idx[len(self.geography_variables):]
        flat_idx = np.ravel_multi_index(idx, self.data_shape)
        # size = np.prod(self.data_shape)
        # tmp = ss.csr_matrix((1,size))
        # tmp[0,flat_idx] = val
        # return blk, tmp
        return blk, {flat_idx: val}

    def make_spar(self, d):
        size = np.prod(self.data_shape)
        spar = ss.dok_matrix((1, size), dtype=int)
        for k, v in d.items():
            spar[0, k] = v
        return spar.tocsr()

    def process(self, data):
        """
            args:
                a Spark dataframe containing CEF person records

            This function performs the following process:
                (1) Convert the data to an RDD.
                (2) row -> (geo_histogram,1)) (Map the row to a geo_histogram, a tuple of the
                    geography variables plus the histogram variables.)
                (3) Reduce by key.
                (4) (geo_histogram,cnt) -> (geo, (histogram, cnt))
                (5) groupbykey: creates (geo, list of (histogram, cnt))
                (6) (geo, list of (histogram, cnt)) -> (geo, ndarray)

            returns: an rdd of (geo,numpy ndarray) pairs.
        """

        def combdict(d1, d2):
            d1.update(d2)
            return d1

        data = self.repartitionData(data)
        #print(f"Rows (person) {das_utils.rddPartitionDistributionMoments(data.rdd)}")

        return (data.rdd.map(self.create_key_value_pair)
                        .reduceByKey(add)
                        .map(self.to_by_geo)
                        .reduceByKey(combdict).mapValues(self.make_spar))


class UnitFromPersonRepartitioned(UnitFromPersonTable, TableWithGeocode):
    """
    Just add the DataFrame repartitioning before processing
    """

    def process(self, data):
        """
            Input:
                 data: a Spark dataframe (df)

            Output:
                a RDD with block by block counts of housing units and gqs by type
        """

        data = self.repartitionData(data)
        return super().process(data)
