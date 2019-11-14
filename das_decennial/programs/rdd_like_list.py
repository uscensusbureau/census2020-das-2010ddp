"""
A list that has the same API as Spark RDD, but does everything locally and sequencially, without need for Spark Context

Usage:
    If you have a piece of program flow where you want to have an easier access into
    processes within map or reduce operations, by having them run locally (sequentially), on the master, rather than executor processes (in parallel),
    and directly see their output printed, be able to set debugging breakpoints, etc.
        ....
        rdd1 = rdd0.map(...)
        ...
        rddN = rddNm1.reduce(...)
        ....
    convert the RDD at the starting monitoring point to RDDLikeList, and convert
    the RDD at the end of monitoring back to regular RDD:
        ....
        rdd1 = RDDLikeList(rdd0).map(...)
        ...
        rddN = rddNm1.reduce(...).RDD()
        ....

"""

import functools
from typing import Iterable
from copy import deepcopy
from collections import defaultdict

import pyspark.rdd
from pyspark.sql.dataframe import DataFrame

def emptydec(rdd_operation):
    """ Empty decorator. Maybe something will be needed later"""
    def wrapper(*args, **kwargs):
        ans = rdd_operation(*args, **kwargs)
        return ans

    return wrapper


#class RDDLikeList(pyspark.rdd.RDD):
class RDDLikeList:
    def __init__(self, iterable: Iterable):
        self.list = list(iterable)
        # self.list = list(map(deepcopy, iterable))
    # def RDD(self):
    #     spark = SparkSession.builder.getOrCreate()
    #     return spark.sparkContext.parallelize(self.list)

    @emptydec
    def collect(self):
        return self.list.copy()

    @emptydec
    def count(self):
        return len(self.list)

    @emptydec
    def map(self, f, preservesPartitioning=False):
        return RDDLikeList(map(f, self.collect()))

    @emptydec
    def flatMap(self, f, preservesPartitioning=False):
        l = [f(el) for el in self.collect()]
        flat_list = [item for sublist in l for item in sublist]
        return RDDLikeList(flat_list)

    @emptydec
    def filter(self, f):
        return RDDLikeList(filter(f, self.collect()))

    @emptydec
    def distinct(self, numPartitions=None):
        raise NotImplementedError

    @emptydec
    def sample(self, withReplacement, fraction, seed=None):
        raise NotImplementedError

    @emptydec
    def randomSplit(self, weights, seed=None):
        raise NotImplementedError

    @emptydec
    def groupBy(self, f, numPartitions=None, partitionFunc=pyspark.rdd.portable_hash):
        raise NotImplementedError

    @emptydec
    def reduceByKey(self, func, numPartitions=None, partitionFunc=pyspark.rdd.portable_hash):
        grouped_by_key = defaultdict(list)
        for k, v in self.collect():
            grouped_by_key[k].append(v)

        reduced = []
        for k, values in grouped_by_key.items():
            reduced_values = deepcopy(values[0]) if len(values) == 1 else functools.reduce(func, values)
            reduced.append((k, reduced_values))

        return RDDLikeList(reduced)

    @emptydec
    def reduce(self, f):
        values = self.collect()
        reduced_value = deepcopy(values[0]) if len(values) == 1 else functools.reduce(f, values)
        return reduced_value

    @emptydec
    def reduceByKeyLocally(self, func):
        raise NotImplementedError

    @emptydec
    def join(self, other, numPartitions=None):
        ans = []
        for sk, sv in self.collect():
            for ok, ov in other.collect():
                if sk == ok:
                    ans.append((sk, (sv, ov)))
        return RDDLikeList(ans)

    @emptydec
    def leftOuterJoin(self, other, numPartitions=None):
        raise NotImplementedError

    @emptydec
    def rightOuterJoin(self, other, numPartitions=None):
        raise NotImplementedError

    @emptydec
    def fullOuterJoin(self, other, numPartitions=None):
        raise NotImplementedError

    @emptydec
    def partitionBy(self, numPartitions, partitionFunc=pyspark.rdd.portable_hash):
        return self

    @emptydec
    def combineByKey(self, createCombiner, mergeValue, mergeCombiners,
                     numPartitions=None, partitionFunc=pyspark.rdd.portable_hash):
        raise NotImplementedError

    @emptydec
    def aggregateByKey(self, zeroValue, seqFunc, combFunc, numPartitions=None,
                       partitionFunc=pyspark.rdd.portable_hash):
        raise NotImplementedError

    @emptydec
    def foldByKey(self, zeroValue, func, numPartitions=None, partitionFunc=pyspark.rdd.portable_hash):
        raise NotImplementedError

    @emptydec
    def groupByKey(self, numPartitions=None, partitionFunc=pyspark.rdd.portable_hash):
        return self

    @emptydec
    def flatMapValues(self, f):
        # TODO: Not tested
        l = [(k, f(v)) for k, v in self.collect()]
        flat_list = [item for sublist in l for item in sublist]
        return RDDLikeList(flat_list)

    @emptydec
    def mapValues(self, f):
        # TODO: Not tested
        return RDDLikeList([(k, f(v)) for k, v in self.collect()])

    @emptydec
    def groupWith(self, other, *others):
        raise NotImplementedError

    @emptydec
    def cogroup(self, other, numPartitions=None):

        # grouped_by_key = defaultdict(list)
        # for k, v in self.collect() + other.collect():
        #     grouped_by_key[k].append(v)

        grouped_by_key = defaultdict(lambda: ([],[]))
        for k, v in self.collect():
            grouped_by_key[k][0].append(v)

        for k, v in other.collect():
            grouped_by_key[k][1].append(v)

        return RDDLikeList([(k, v) for k, v in grouped_by_key.items()])

    @emptydec
    def union(self, other):
        return RDDLikeList(self.collect() + other.collect())

    @emptydec
    def sampleByKey(self, withReplacement, fractions, seed=None):
        raise NotImplementedError

    @emptydec
    def subtractByKey(self, other, numPartitions=None):
        raise NotImplementedError

    @emptydec
    def subtract(self, other, numPartitions=None):
        raise NotImplementedError

    @emptydec
    def keyBy(self, f):
        raise NotImplementedError

    @emptydec
    def repartition(self, numPartitions):
        return self

    @emptydec
    def coalesce(self, numPartitions, shuffle=False):
        return self

    @emptydec
    def zip(self, other):
        return RDDLikeList(zip(self.collect(), other.collect()))

    @emptydec
    def zipWithIndex(self):
        return RDDLikeList(enumerate(self.collect()))

    @emptydec
    def zipWithUniqueId(self):
        raise NotImplementedError

    @emptydec
    def persist(self, storageLevel=None):
        return self

    @emptydec
    def unpersist(self):
        return self

    @emptydec
    def take(self, num):
        return self.list[:num]

    @emptydec
    def getNumPartitions(self):
        return 1

    @emptydec
    def persist(self, *args):
        return self

    @emptydec
    def unpersist(self):
        return self

    @emptydec
    def aggregate(self, zeroValue, seqOp, combOp):

        acc = zeroValue
        for el in self.collect():
            acc = seqOp(acc, el)

        return acc

        # vals = self.map(func).collect()
        # return functools.reduce(combOp, vals, zeroValue)


class DataFrameLikeList(DataFrame):

    def __init__(self, df: DataFrame):
        self._jdf = df._jdf
        self.sql_ctx = df.sql_ctx
        self._sc =df._sc
        self.is_cached = df.is_cached
        self._schema = df._schema
        self._lazy_rdd = df._lazy_rdd

    @property
    def rdd(self):
        return RDDLikeList(super().rdd)

    @emptydec
    def repartition(self, numPartitions, *cols):
        return DataFrameLikeList(super().repartition(numPartitions, *cols))

    @emptydec
    def withColumn(self, colName, col):
        return DataFrameLikeList(super().withColumn(colName, col))
