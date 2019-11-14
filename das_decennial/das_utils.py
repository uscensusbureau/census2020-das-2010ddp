# Useful AWS CLI commands
# 
# aws s3 ls s3://uscb-decennial-ite-das/
#
# aws s3 rm {path} --recursive --exclude * --include {regex/wildcard/name}
#
# aws s3 cp {from_path} {to_path} --recursive --exclude * --include {regex/wildcard/name} --quiet
#

#####################
# Possibly useful notes:
# https://stackoverflow.com/questions/36994839/i-can-pickle-local-objects-if-i-use-a-derived-class
#####################

import sys
import json
import pickle
import os
import subprocess
import gc
import re
import logging
import zipfile
import glob
import scipy.sparse
import functools

from typing import Union
from operator import add
from configparser import ConfigParser, NoOptionError, NoSectionError

import numpy as np
import time
import datetime
import types

from programs import sparse as sparse
import das_framework.ctools.s3 as s3
import constants as C
from constants import CC

from das_framework.ctools.exceptions import DASValueError

DELIM = C.REGEX_CONFIG_DELIM

def getSafeQueryname(queryname):
    """
    Since the asterisk typically has special meaning in file systems, it's usually not used
    in filenames. As such, our standard (schema-based) query naming convention for crosses of
    queries "q1 * q2" needs to be renamed to something else. In this case, we will use a period
    to signify crosses in the filename: "q1.q2"
    """
    return ".".join(re.split(C.SCHEMA_CROSS_SPLIT_DELIM, queryname))


def getStandardQueryname(safe_queryname):
    """
    Convert from a safe queryname standard "q1.q2" to the schema standard "q1 * q2"
    """
    return C.SCHEMA_CROSS_JOIN_DELIM.join(safe_queryname.split("."))


@functools.lru_cache()
def getMasterIp():
    """IP Address of EMR Master Node"""
    return json.loads(open(CC.JOB_FLOW_JSON).read())['masterPrivateDnsName']


def isodate(sep="-"):
    iso = datetime.date.isoformat(datetime.date.fromtimestamp(time.time()))
    return sep.join(iso.split("-"))


def timestamp(seconds):
    """
    returns an approximate time that has been formatted to read more nicely for long runs
    
    Inputs:
        seconds: the number of seconds
    
    Outputs:
        a string representation of the time, as expressed in days, hours, minutes, and seconds
    
    Notes:
        in order to increase readability, seconds is cast to an int, so the times expressed are only approximate (+/- a second)
    """
    seconds = int(seconds)
    if seconds < 1:
        items = ["less than 1 second"]
    else:
        days, seconds = divmod(seconds, 24*60*60)
        hours, seconds = divmod(seconds, 60*60)
        minutes, seconds = divmod(seconds, 60)
        items = [
            f"{days} days" if days > 1 else f"{days} day" if days > 0 else "",
            f"{hours} hours" if hours > 1 else f"{hours} hour" if hours > 0 else "",
            f"{minutes} minutes" if minutes > 1 else f"{minutes} minute" if minutes > 0 else "",
            f"{seconds} seconds" if seconds > 1 else f"{seconds} second" if seconds > 0 else ""
        ]
    
    items = [x for x in items if x != ""]
    
    return ", ".join(items)


def pretty(item, indent=4, join_with='\n'):
    if type(item) == dict:
        return json.dumps(item, indent=indent)
    elif isinstance(item, np.ndarray):
        item = item.tolist()
        return join_with.join(item)
    else:
        item = aslist(item)
        return join_with.join(item)


def aslist(item):
    """
    Wraps a single value in a list, or just returns the list
    """
    if isinstance(item, list):
        value = item
    elif isinstance(item, range):
        value = list(item)
    else:
        value = [item]
    
    return value


def tolist(item):
    """
    Converts the item to a list, or just returns the list
    """
    return item if type(item) == list else list(item)


def quickRepr(item, join='\n'):
    items = ["{}: {}".format(attr, value) for attr, value in item.__dict__.items()]
    if join is None:
        return items
    else:
        return join.join(items)


def getObjectName(obj):
    return obj.__qualname__


def runpathSort(runpaths):
    runpaths_sorted = runpaths.copy()
    runpaths_sorted.sort(key=lambda s: int(s.split("/")[-2].split("_")[1]))
    return runpaths_sorted


def printList(thelist):
    print('\n\n'.join([str(x) for x in thelist]))


def flattenList(nested_list):
    nested_list = nested_list.copy()
    flattened_list = []
    while nested_list != []:
        element = nested_list.pop()
        if type(element) == list:
            nested_list += element
        else:
            flattened_list.append(element)
    
    flattened_list.reverse()
    return flattened_list


#################
# Utility functions
# I/O
#################

def loadConfigFile(path):
    config = ConfigParser()
    # since the config parser object automatically converts item names to lowercase,
    # use this to prevent it from doing so
    config.optionxform = str
    if isS3Path(path):
        config_file = s3.s3open(path=path, mode="r")
        # config.readfp(config_file)  This is deprecated
        config.read_file(config_file)
        config_file.close()
    else:
        with open(path, 'r') as config_file:
            # config.readfp(config_file)  This is deprecated
            config.read_file(config_file)
    
    return config


def getGeoDict(config):
    assert 'geodict' in config, "This config file doesn't contain a 'geodict' section."
    keys = ['geolevel_names', 'geolevel_leng']
    geodict = {}
    for k in keys:
        geodict[k] = config['geodict'][k]
    return geodict


def makePath(path):
    if not os.path.exists(path):
        os.makedirs(path)

def clearPath(path):
    # Remove what's at [path]
    logging.info(f"clearPath({path})")
    if isS3Path(path):
        if path.count('/') < 4:
            raise RuntimeError(f"path '{path}' doesn't have enough /'s in it to allow recursive rm")
        subprocess.call(['aws','s3','rm','--recursive','--quiet',path])
    else:
        subprocess.run(['hadoop', 'fs', '-rm', '-r', path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def loadPickleFile(path):
    contents = None
    if isS3Path(path):
        contents = loadPickleS3(path)
    else:
        with open(path, 'rb') as f:
            contents = pickle.load(f)
    
    return contents


def loadJSONFile(path):
    contents = None
    if isS3Path(path):
        contents = loadJSONS3(path)
    else:
        with open(path, 'r') as f:
            contents = json.load(f)
        
    return contents


def loadFromS3(path):
    ext = path.split('.')[1]
    if ext == "json":
        contents = loadJSONS3(path)
    else:
        contents = loadPickleS3(path)
    
    return contents


def loadJSONS3(path):
    jsonfile = s3.s3open(path=path, mode='r')
    contents = json.load(jsonfile.file_obj)
    return contents


def loadPickleS3(path):
    loadfile = s3.s3open(path=path, mode="rb")
    contents = pickle.load(loadfile.file_obj)
    return contents


def isS3Path(path):
    """
    isS3Path does a simple check to see if the path looks like an s3 path or not
    
    Notes:
        it checks to see if the path string has the standard s3 prefix "s3://"
        at the beginning
    """
    return path.startswith(C.S3_PREFIX)


def saveNestedListAsTextFile(path, thelist):
    thelist = flattenList(thelist)
    saveListAsTextFile(path, thelist)


def saveListAsTextFile(path, thelist, mode="w"):
    if isS3Path(path):
        savefile = s3.s3open(path=path, mode=mode)
        for item in thelist:
            savefile.write("{}\n".format(item))
        savefile.close()
    else:
        with open(path, mode) as f:
            for item in thelist:
                f.write("{}\n".format(item))


def saveConfigFile(path, config):
    # Only save the config file in S3.
    # It's in the DFXML file anyway.
    if isS3Path(path):
        savefile = s3.s3open(path=path, mode="w")
        config.write(savefile)
        savefile.close()
    else:
        logging.warning(f"saveConfigFile: not saving config file to {path}")

def saveJSONFile(path, data, indent=None):
    if isS3Path(path):
        savefile = s3.s3open(path=path, mode="w")
        json.dump(data, savefile, indent=indent)
        savefile.close()
    else:
        with open(path, 'w') as f:
            json.dump(data, f, indent=indent)


def expandPathRemoveHdfs(path):
    return os.path.expandvars(path).replace(C.HDFS_PREFIX,"")

def savePickledRDD(path, rdd, batch_size=10):
    path = expandPathRemoveHdfs(path)
    clearPath(path)
    logging.info(f"rdd.saveAsPickleFile({path},batchSize={batch_size})")
    rdd.saveAsPickleFile(path, batchSize=batch_size)


def savePickleFile(path, data):
    path = expandPathRemoveHdfs(path)
    if isS3Path(path):
        savefile = s3.s3open(path=path, mode='wb')
        pickle.dump(data, savefile)
        savefile.close()
    else:
        with open(path, 'wb') as f:
            pickle.dump(data, f)


def freeMemRDD(some_rdd):
    """ Remove RDD from memory, with garbage collection"""
    some_rdd.unpersist()
    del some_rdd
    gc.collect()


def partitionByParentGeocode(block_nodes, num_partitions):
    """ Partition an RDD of geonodes by parent geocode """
    print("Repartitioning by parent geocode...")
    parent_counts = block_nodes.map(lambda node: (node.parentGeocode, 1)).reduceByKey(add).collect()
    parent_codes = sorted(map(lambda d: (lambda key, count: key)(*d), parent_counts))
    return block_nodes \
        .map(lambda node: (node.parentGeocode, node)) \
        .partitionBy(num_partitions, lambda k: parent_codes.index(k)) \
        .map(lambda d: (lambda parent_code, node: node)(*d))


def splitBySize(code_count_list):
    # Sort (code, children_count) list by children count
    a_s = sorted(code_count_list, key=lambda d: (lambda geocode, count: int(count))(*d))

    # Find maximal children count
    a_max = int(a_s[-1][1])

    # Counter to go over a list
    i = 0

    # Partition to assign the node to
    partition = 0

    # {geocode: partition} dictionary
    pdict = {}

    while i < len(a_s):  # Go over list, node by node
        s = 0  # Sum of children in current partition
        while s < 0.7 * a_max:  # Loop over one partition, trying to get in between 0.7 and 1.1 of maximal
            s += int(a_s[i][1])  # Increase number of children already in partition
            pdict[a_s[i][0]] = partition  # Assign node to this partition
            i += 1
        if s > 1.1 * a_max:    # If we overshot, remove the last node from the partition
            i -= 1
            s -= int(a_s[i][1])
        partition += 1     # Proceed to the next partition
    return pdict


def partitionBySiblingNumber(block_nodes, num_partitions):
    """ Partition an RDD of geonodes by number of geounits with the same parent """
    print("Repartitioning by number of siblings...")
    parent_counts = block_nodes.map(lambda node: (node.parentGeocode, 1)).reduceByKey(add).collect()
    pdict = splitBySize(parent_counts)
    return block_nodes \
        .map(lambda node: (node.parentGeocode, node)) \
        .partitionBy(num_partitions, lambda k: pdict[k]) \
        .map(lambda d: (lambda parent_code, node: node)(*d))


def rddPartitionDistributionMoments(rdd):
    """Return mean and standard deviation of sizes (number of elements) of partitions of the RDD"""
    sizes = rdd.glom().map(lambda d: len(d)).collect()
    return f"in {len(sizes)} partitions: {np.mean(sizes) - np.std(sizes)}, {np.mean(sizes) +np.std(sizes)}"


def ship_files2spark(spark, tempdir, allfiles=False, subdirs=('programs', 'das_framework', 'otherconsts'), subdirs2root=()):
    """
    Zips the files in das_decennial folder and indicated subfolders to have as submodules and ships to Spark
    as zip python file.
    Also can ship as a regular file (for when code looks for non-py files)
    :param subdirs:  Subdirectories to add to the zipfile
    :param allfiles: whether to ship all files (as opposed to only python (.py) files)
    :param tempdir: directory for zipfile creation
    :param spark: SparkSession where to ship the files
    :return:
    """
    # das_decennial directory
    ddecdir = os.path.dirname(__file__)
    zipf = zipfile.ZipFile(os.path.join(tempdir, 'das_decennial_submodules.zip'), 'w', zipfile.ZIP_DEFLATED)

    # File with extension to zip
    ext = '*' if allfiles else 'py'

    # Add the files to zip, keeping the directory hierarchy
    for submodule_dir in subdirs:
        files2ship = [fn for fn in glob.iglob(os.path.join(ddecdir, f'{submodule_dir}/**/*.{ext}'), recursive=True)]

        for fname in files2ship:
            zipf.write(os.path.join(ddecdir, fname),
                       arcname=submodule_dir.split('/')[-1] + fname.split(submodule_dir)[1])

    # Add files in the das_decennial directory
    for fullname in glob.glob(os.path.join(ddecdir, f'*.{ext}')):
        zipf.write(fullname, arcname=os.path.basename(fullname))

    # Add files that are imported as if from root
    for subdir2root in subdirs2root:
        for fullname in glob.glob(os.path.join(ddecdir, subdir2root, f'*.{ext}')):
            zipf.write(fullname, arcname=os.path.basename(fullname))

    zipf.close()

    spark.sparkContext.addPyFile(zipf.filename)

    if allfiles:
        spark.sparkContext.addFile(zipf.filename)


def class_from_config(config: ConfigParser, key, section):
    """
    Import module and return class from it, based on option in the config file
    :param config: ConfigParser with loaded config file
    :param key: config option within section, containing filename with module and the class name within, dot-separated
    :param section: section of the config file
    :return:
    """
    try:
        (file, class_name) = config.get(section, key).rsplit(".", 1)
    except (NoSectionError, NoOptionError) as e:
        err_msg = f"Key \"{key}\" in config section [{section}] not found when specifying module to load\n{e}"
        logging.error(err_msg)
        raise KeyError(err_msg)

    try:
        module = __import__(file, fromlist=[class_name])
    except ImportError as e:
        err_msg = f"Module {file} import failed.\nCurrent directory: {os.getcwd()}\nFile:{__file__}\nsys.path:{sys.path}\n{e.args[0]}"
        logging.error(err_msg)
        raise ImportError(err_msg)

    try:
        c = getattr(module, class_name)
    except AttributeError as e:
        err_msg = f"Module {module} does not have class \"{class_name}\" which is indicated in config [{section}]/{key} option to be loaded as a module for DAS\n{e.args[0]}"
        logging.error(err_msg)
        raise AttributeError(err_msg)

    return c


def table2hists(d, schema, housing_varname=None, units=False):
    """
    Returns person and housing histograms from a person(or household) table :d: with a unique housingID as last column,
    or just housing histogram from a unit table :d:
    The columns except for the last are according to the :schema:

    Intended for use mainly in the unit tests

    :param d: person(or household) table, a row per person, columns according to the schema + 1 column with a unique housing unit UID, alternatively
        housing unit table, a row per unit, colums according to unit schema + 1 (usually just two columns, unit HH/GQtype + UID)
    :param schema: schema to be used for histogram creation (PL94, SF1 etc.), as programs.schema.schema
    :param housing_varname: name of housing variable in the schema (usually 'rel' or 'hhgq'). Might be temporary until we converge on a single name
    :return: (p_h, h_h), a tuple of das_decennial.programs.sparse.multiSparse arrays, for person and housing histograms respectively
    """
    assert isinstance(d, np.ndarray), "The person/unit data is not passed as numpy array (likely, as a list instead)"

    # This is person/household histogram if d is person/household table and units is False, and unit histogram if d is unit table and units is True
    p_h = np.histogramdd(d[:, :-1], bins=schema.shape, range=tuple([0, ub - 1] for ub in schema.shape))[0]
    if units:
        # Return it as housing units histogram
        return sparse.multiSparse(p_h.astype(int))

    if housing_varname is not None:  # Make units table from units UID and make histogram
        hhgq_ind = schema.dimnames.index(housing_varname)
        h_h = np.histogram(d[np.unique(d[:, -1], return_index=True)[1], hhgq_ind], bins=schema.shape[hhgq_ind], range=[0, schema.shape[hhgq_ind] - 1])[0]
        return sparse.multiSparse(p_h.astype(int)), sparse.multiSparse(h_h.astype(int))

    # Otherwise we're only interested in person/household histogram, and get units histogram with a separate call to this function
    return sparse.multiSparse(p_h.astype(int))


def npArray(a: Union[np.ndarray, sparse.multiSparse], shape=None) -> np.ndarray:
    """
    If argument is a multiSpare, return as numpy array; if numpy array return itself
    :param a:
    :return:
    """
    if isinstance(a, sparse.multiSparse):
        return a.toDense()

    if isinstance(a, np.ndarray):
        return a

    if shape is not None:
        if isinstance(a, scipy.sparse.csr_matrix):
            return a.toarray().reshape(shape)

    raise TypeError("Neither numpy array, nor multiSparse, nor csr_matrix with shape provided")

def int_wlog(s, name):
    """
    Convert a string to integer, wrapped so that it issues error to the logging module, with the variable name
    :param s: string to convert
    :param name: variable name, to indicate in the error message
    :return: integer obtained as conversion of s
    """
    try:
        return int(s)
    except ValueError as err:
        error_msg = f"{name} value '{s}' is not numeric, conversion attempt returned '{err.args[0]}'"
        raise DASValueError(error_msg, s)
