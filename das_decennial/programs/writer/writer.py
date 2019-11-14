"""
This module implements DAS Decennial 2020 generic writer class,
common functionality for all writers.
New writers should be inherited from this class or its descendants

"""

#######################################################
# Other attributes and options:
# output_path      : the output path where the data will be stored
#                     automatically detects if s3 path or not

# output_datafile_name: what is added to path for data saving
#
# produce_flag      : whether or not to write the data to file
#                     Use 1 to save / 0 to not save the data
#
# num_parts         : indicates how to repartition the rdd for faster saving
#                     default = 100
#
#######################################################
# For quick copying:
#
# [writer]
# writer:
# output_path:
# output_datafile_name:
# produce_flag:
# num_parts:
#
#######################################################


import datetime
import logging
import os
import os.path
import psutil
import pwd
import sys
import time

import constants as C
from das_framework.ctools.s3 import s3open
import das_framework.driver as driver
import das_utils
import programs.s3cat as s3cat

from abc import ABCMeta, abstractmethod


class DASDecennialWriter(driver.AbstractDASWriter, metaclass=ABCMeta):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Whether to produce output data
        self.produce = self.getboolean(C.PRODUCE)
        # Where to save the data
        self.output_path = self.getconfig(C.OUTPUT_PATH, expandvars=True)
        # Output file name
        self.output_datafname = self.getconfig(C.OUTPUT_DATAFILE_NAME, default='data')
        # Whether to overwrite the data if the file already exists
        self.overwrite_flag = self.getboolean(C.OVERWRITE_FLAG, default=False)
        # Whether to write metadata
        self.write_metadata = self.getboolean(C.WRITE_METADATA, section=C.WRITER, default=C.WRITE_METADATA_DEFAULT)
        # Whether to concatenate the parts files in S3 into a single file
        self.s3cat = self.getboolean(C.S3CAT, section=C.WRITER, default=C.S3CAT_DEFAULT)
        # Suffix to add to that concatenated file
        self.s3cat_suffix = self.getconfig(C.S3CAT_SUFFIX, default='')
        # Verbose mode of s3cat program
        self.s3cat_verbose = self.getboolean(C.S3CAT_VERBOSE, default=False)

    def setOutputFileDataName(self, value):
        self.output_datafname = value

    def unsetOverwriteFlag(self):
        self.overwrite_flag = False

    def saveMetadata(self, *, path, now, count):
        """Saves metadata to the requested S3 location. This header will then be combined with the contents by the s3cat command"""
        logging.info(f"writing metadata to {path} {now} count={count}")
        with s3open(path, "w", fsync=True) as f:
            f.write("# {}\n".format(self.getconfig(C.CLASSIFICATION_LEVEL, section=C.WRITER, default=C.DEFAULT_CLASSIFICATION_LEVEL)))
            f.write("# Created: {}\n".format(now))
            f.write("# Records: {}\n".format(count))
            f.write("# Command line: {}\n".format(sys.executable + " " + " ".join(sys.argv)))
            f.write("# uid: {}\n".format(os.getuid()))
            f.write("# username: {}\n".format(pwd.getpwuid(os.getuid())[0]))
            f.write("# Boot Time: {}\n".format(datetime.datetime.fromtimestamp(psutil.boot_time()).isoformat()))
            f.write("# Start Time: {}\n".format(datetime.datetime.fromtimestamp(self.das.t0).isoformat()))
            uname = os.uname()
            uname_fields = ['os_sysname', 'host', 'os_release', 'os_version', 'arch']
            for i in range(len(uname_fields)):
                f.write("# {}: {}\n".format(uname_fields[i], uname[i]))

    def saveHeader(self, *, path):
        """overwrite to save header elements"""
        pass

    def write(self, engine_tuple):
        self.annotate(f"{self.__class__.__name__} write")
        blocknoderdd, feas_dict = engine_tuple

        # keep the original rdd so the validator can still check that the constraints have been met
        # original_rdd = blocknoderdd

        if self.produce:
            self.annotate("Producing DAS output", verbose=True)
            rdd     = self.transformRDDForSaving(blocknoderdd)
            rdd.count()
            start_time = time.time()
            num_parts = self.getint(C.NUM_PARTS, default=100)
            if num_parts > 0:
                rdd = rdd.coalesce(num_parts).persist()
            else:
                self.annotate(f"Writer coalesce skipped because num_parts <= 0", verbose=True)
            self.annotate(f"num_parts={num_parts} time for repartition/coalesce: {time.time() - start_time}", verbose=True)
            self.saveRunData(self.output_path, feas_dict=feas_dict, rdd=rdd)
            self.annotate(f"num_parts={num_parts} time for saveRunData: {time.time() - start_time}", verbose=True)

        self.annotate(f"{self.__class__.__name__}.write done")
        return blocknoderdd

    @abstractmethod
    def transformRDDForSaving(self, rdd):
        pass

    def saveRunData(self, path, feas_dict=None, rdd=None):
        self.annotate("saveRunData", verbose=True)
        if path[-1] == '/':
            path = path[0:-1]

        # RDD must be saved first, because it needs an empty prefix.
        if rdd is not None:
            output_datafile_name      = os.path.join(path, self.output_datafname)

            if self.overwrite_flag:
                das_utils.clearPath(output_datafile_name)

            # needed when not an s3 path, as the with open context assumes the folder already exists
            if not das_utils.isS3Path(output_datafile_name):
                das_utils.makePath(output_datafile_name)

            output_metadata_file_name = output_datafile_name+"/0_metadata"  # sorts before 'p'
            output_header_file_name   = output_datafile_name+"/1_header"    # sorts before 'p' but after '1'
            self.annotate(f"writing RDD to {output_datafile_name}")
            self.saveRDD(output_datafile_name, rdd)

            if self.write_metadata:
                now = datetime.datetime.now().isoformat()
                self.saveMetadata(path=output_metadata_file_name, now=now, count=rdd.count())
                self.saveHeader(path=output_header_file_name)

            if self.s3cat:
                self.annotate(f"combining {output_datafile_name} with s3cat")
                s3cat.s3cat(output_datafile_name,
                            demand_success=True,
                            suffix=self.s3cat_suffix,
                            verbose=self.s3cat_verbose)
                self.add_output_path(output_datafile_name + self.s3cat_suffix)
            else:
                self.add_output_path(output_datafile_name)

        config_path = os.path.join(path, C.CONFIG_INI)
        self.annotate("Saving config to directory: {}".format(config_path))
        das_utils.saveConfigFile(config_path, self.config)

        if feas_dict is not None:
            for key in feas_dict.keys():
                if hasattr(feas_dict[key], 'value'):
                    feas_dict[key] = feas_dict[key].value  # this seems redundant, but is actually needed for the accumulator
            self.log_and_print(f"Feasibility dictionary: {feas_dict}")
            feas_path = os.path.join(path, C.FEAS_DICT_JSON)
            self.annotate(f"Saving feas_dict to directory: {feas_path}")
            das_utils.saveJSONFile(feas_path, feas_dict)

    @abstractmethod
    def saveRDD(self, path, rdd):
        pass
