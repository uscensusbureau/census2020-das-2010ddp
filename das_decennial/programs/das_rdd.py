"""
Class to add custom functionality to RDDs, for testing and measuring purposes

Usage:
    If you have a piece of program flow where you want to time RDD operations,
        ....
        rdd1 = rdd0.map(...)
        ...
        rddN = rddNm1.reduce(...)
        ....
    convert the RDD at the starting monitoring point to DASRDD, and convert
    the RDD at the end of monitoring back to regular RDD:
        ....
        rdd1 = DASRDD(rdd0).map(...)
        ...
        rddN = rddNm1.reduce(...).RDD()
        ....
    Then everything that happens with the chain of RDDs between rdd0 and rddN will run with
    the @timeit decorator, which forces lazy evaluation to evaluate and measures and prints time along with
    the place in code where it occured.
"""

import time
import logging
import os
from inspect import getouterframes, currentframe

import pyspark.rdd
from pyspark.sql.dataframe import DataFrame

def timeit(rdd_operation):
    """ Decorator to force evaluation after each RDD operation and measure time"""
    def wrapper(*args, **kwargs):
        frameinfo = getouterframes(currentframe())[1]
        t0 = time.time()
        ans = rdd_operation(*args, **kwargs)
        # Don't want to measure when the functions are called by pyspark istelf (like mapPartitionsWithIndex is called from map), only from our code
        if os.path.basename(os.path.dirname(frameinfo.filename)) != "pyspark":
            ans.count()
            #t = ans.collect()[0]
            msg = f"\t{os.path.basename(frameinfo.filename):20}:{frameinfo.lineno:4} {time.time() - t0:7.2f} s ({rdd_operation.__name__:15s}), " \
                f"{frameinfo.code_context[0].strip()}"
            print(msg)
            logging.debug(msg)
        return ans

    return wrapper


class DASRDD(pyspark.rdd.RDD):
    def __init__(self, rdd: pyspark.rdd.RDD):
        self._jrdd = rdd._jrdd
        self.is_cached = rdd.is_cached
        self.is_checkpointed = rdd.is_checkpointed
        self.ctx = rdd.ctx
        self._jrdd_deserializer = rdd._jrdd_deserializer
        self._id = rdd._id
        self.partitioner = rdd.partitioner

    def RDD(self):
        return pyspark.rdd.RDD(self._jrdd, self.ctx, jrdd_deserializer=self._jrdd_deserializer)

    @timeit
    def map(self, f, preservesPartitioning=False):
        return DASRDD(super().map(f, preservesPartitioning))

    @timeit
    def flatMap(self, f, preservesPartitioning=False):
        return DASRDD(super().flatMap(f, preservesPartitioning))

    @timeit
    def filter(self, f):
        return DASRDD(super().filter(f))

    @timeit
    def distinct(self, numPartitions=None):
        return DASRDD(super().distinct(numPartitions))

    @timeit
    def sample(self, withReplacement, fraction, seed=None):
        return DASRDD(super().sample(withReplacement, fraction, seed))

    @timeit
    def randomSplit(self, weights, seed=None):
        return DASRDD(super().randomSplit(weights, seed))

    @timeit
    def groupBy(self, f, numPartitions=None, partitionFunc=pyspark.rdd.portable_hash):
        return DASRDD(super().groupBy(f, numPartitions, partitionFunc))

    @timeit
    def reduceByKey(self, func, numPartitions=None, partitionFunc=pyspark.rdd.portable_hash):
        return DASRDD(super().reduceByKey(func, numPartitions, partitionFunc))

    @timeit
    def reduceByKeyLocally(self, func):
        return DASRDD(super().reduceByKeyLocally(func))

    @timeit
    def join(self, other, numPartitions=None):
        return DASRDD(super().join(other, numPartitions))

    @timeit
    def leftOuterJoin(self, other, numPartitions=None):
        return DASRDD(super().leftOuterJoin(other, numPartitions))

    @timeit
    def rightOuterJoin(self, other, numPartitions=None):
        return DASRDD(super().rightOuterJoin(other, numPartitions))

    @timeit
    def fullOuterJoin(self, other, numPartitions=None):
        return DASRDD(super().fullOuterJoin(other, numPartitions))

    @timeit
    def partitionBy(self, numPartitions, partitionFunc=pyspark.rdd.portable_hash):
        return DASRDD(super().partitionBy(numPartitions, partitionFunc))

    @timeit
    def combineByKey(self, createCombiner, mergeValue, mergeCombiners,
                     numPartitions=None, partitionFunc=pyspark.rdd.portable_hash):
        return DASRDD(super().combineByKey(createCombiner, mergeValue, mergeCombiners, numPartitions, partitionFunc))

    @timeit
    def aggregateByKey(self, zeroValue, seqFunc, combFunc, numPartitions=None,
                       partitionFunc=pyspark.rdd.portable_hash):
        return DASRDD(super().aggregateByKey(zeroValue, seqFunc, combFunc, numPartitions, partitionFunc))

    @timeit
    def foldByKey(self, zeroValue, func, numPartitions=None, partitionFunc=pyspark.rdd.portable_hash):
        return DASRDD(super().foldByKey(zeroValue, func, numPartitions, partitionFunc))

    @timeit
    def groupByKey(self, numPartitions=None, partitionFunc=pyspark.rdd.portable_hash):
        return DASRDD(super().groupByKey(numPartitions, partitionFunc))

    @timeit
    def flatMapValues(self, f):
        return DASRDD(super().flatMapValues(f))

    @timeit
    def mapValues(self, f):
        return DASRDD(super().mapValues(f))

    @timeit
    def groupWith(self, other, *others):
        return DASRDD(super().groupWith(other, *others))

    @timeit
    def cogroup(self, other, numPartitions=None):
        return DASRDD(super().cogroup(other, numPartitions))

    @timeit
    def sampleByKey(self, withReplacement, fractions, seed=None):
        return DASRDD(super().sampleByKey(withReplacement, fractions, seed))

    @timeit
    def subtractByKey(self, other, numPartitions=None):
        return DASRDD(super().subtractByKey(other, numPartitions))

    @timeit
    def subtract(self, other, numPartitions=None):
        return DASRDD(super().subtract(other, numPartitions))

    @timeit
    def keyBy(self, f):
        return DASRDD(super().keyBy(f))

    @timeit
    def repartition(self, numPartitions):
        return DASRDD(super().repartition(numPartitions))

    @timeit
    def coalesce(self, numPartitions, shuffle=False):
        return DASRDD(super().coalesce(numPartitions, shuffle))

    @timeit
    def zip(self, other):
        return DASRDD(super().zip(other))

    @timeit
    def zipWithIndex(self):
        return DASRDD(super().zipWithIndex())

    @timeit
    def zipWithUniqueId(self):
        return DASRDD(super().zipWithUniqueId())


class DASDataFrame(DataFrame):

    def __init__(self, df: DataFrame):
        self._jdf = df._jdf
        self.sql_ctx = df.sql_ctx
        self._sc =df._sc
        self.is_cached = df.is_cached
        self._schema = df._schema
        self._lazy_rdd = df._lazy_rdd

    @property
    def rdd(self):
        return DASRDD(super().rdd)

    @timeit
    def repartition(self, numPartitions, *cols):
        return DASDataFrame(super().repartition(numPartitions, *cols))

    @timeit
    def withColumn(self, colName, col):
        return DASDataFrame(super().withColumn(colName, col))
