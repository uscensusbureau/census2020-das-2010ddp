# DAS Spark setup module
# William Sexton
# Last modified: 7/12/2018

"""
    This is the setup module for DAS development on Research 2 and on EMR clusters.
    It launches Spark.
"""

import logging
import tempfile
from typing import Tuple, Dict, Any, Callable, List
import numpy

from hdfs_logger import setup as hdfsLogSetup
from programs.constraints.constraint_dict import ConstraintDict
from das_utils import ship_files2spark
from das_framework.driver import AbstractDASSetup
import constants as C
from constants import CC

# Bring in the spark libraries if we are running under spark
# This is done in a try/except so that das_setup.py can be imported for test continious test framework
# even when we are not running under Spark
#
try:
    from pyspark import SparkConf, SparkContext
    from pyspark.sql import SparkSession
except ImportError:
    pass

# Bring in the DAS modules. The debugging is designed to help us understand
# why it's failing sometimes under spark. We can probably clean some of this out.
# For example, why is it looking for the git repository???

# try:
#     from das_framework.driver import AbstractDASSetup
# except ImportError:
#     logging.debug("System path: {}".format(sys.path))
#     logging.debug("Working dir: {}".format(os.getcwd()))
#     path = os.path.realpath(__file__)
#     pathdir = os.path.dirname(path)
#     while True:
#         if os.path.exists(os.path.join(pathdir, ".git")):
#             logging.debug("Found repository root as: {}".format(pathdir))
#             break
#         pathdir = os.path.dirname(pathdir)
#         if os.path.dirname(path) == path:
#             raise ImportError("Could not find root of git repository")
#     path_list = [os.path.join(root, name) for root, dirs, files in os.walk(pathdir)
#                  for name in files if name == "driver.py"]
#     logging.debug("driver programs found: {}".format(path_list))
#     assert len(path_list) > 0, "Driver not found in git repository"
#     assert len(path_list) == 1, "Too many driver programs found"
#     sys.path.append(os.path.dirname(path_list[0]))
#     from das_framework.driver import AbstractDASSetup


# TK - FIXME - Get the spark.eventLog.dir from the config file and set it here, rather than having it set in the bash script
#              Then check to make sure the directory exists.


class DASDecennialSetup(AbstractDASSetup):
    """
        DAS setup class for 2018 development on research 2 and emr clusters.
    """
    levels: Tuple[str, ...]
    schema: str
    hist_shape: Tuple[int, ...]
    hist_vars: Tuple[str, ...]
    postprocess_only: bool
    inv_con_by_level: Dict[str, Dict[str, List[str]]]
    dir4sparkzip: str

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        if self.getboolean(C.PRE_RELEASE, default=False):
            import programs.pre_release_datadict as datadict
        else:
            import programs.datadict as datadict

        self.datadict = datadict

        # Whether to run in spark (there is a local/serial mode, for testing and debugging)
        self.use_spark = self.getboolean(C.SPARK, section=C.ENGINE, default=True)

        # Geographical level names
        self.levels = self.gettuple(C.GEODICT_GEOLEVELS, section=C.GEODICT)
        self.log_and_print(f"geolevels: {self.levels}")

        # schema keyword
        self.schema = self.getconfig(C.SCHEMA, section=C.SCHEMA)
        self.log_and_print(f"schema keyword: {self.schema}")
        self.schema_obj = self.datadict.getSchema(self.schema)
 
        self.postprocess_only = self.getboolean(C.POSTPROCESS_ONLY, section=C.ENGINE, default=False)
        self.validate_input_data_constraints = self.getboolean(C.VALIDATE_INPUT_DATA_CONSTRAINTS, section=C.READER, default=True)

        self.inv_con_by_level = {}
        for level in self.levels:
            self.inv_con_by_level[level] = {
                "invar_names": self.gettuple(f"{C.THEINVARIANTS}.{level}", section=C.CONSTRAINTS, default=()),
                "cons_names": self.gettuple(f"{C.THECONSTRAINTS}.{level}", section=C.CONSTRAINTS, default=())
            }
        try:
            # Person table histogram shape (set here and then checked/set in the reader module init)
            self.hist_shape = self.schema_obj.shape

            # Person table histogram variables (set here and then checked/set in the reader module init)
            self.hist_vars = self.schema_obj.dimnames
        except AssertionError:
            self.log_warning_and_print(f"Schema {self.schema} is not supported")

        # Temporary directory with code and files shipped to spark, to delete later
        self.dir4sparkzip = None

    def makeInvariants(self, *args, **kwargs):
        return self.datadict.getInvariantsModule(self.schema).InvariantsCreator(*args, **kwargs).calculateInvariants().invariants_dict

    def makeConstraints(self, *args, **kwargs):
        return self.datadict.getConstraintsModule(self.schema).ConstraintsCreator(*args, **kwargs).calculateConstraints().constraints_dict

    def setup_func(self):
        """
            Starts spark up in local mode or client mode for development and testing on Research 2.
        """

        # If we are making the BOM, just return
        if self.das.make_bom_only():
            return self

        # Validate the config file. DAS Developers: Add your own!
        pyspark_log = logging.getLogger('py4j')
        pyspark_log.setLevel(logging.ERROR)
        hdfsLogSetup(self.config)
        conf = SparkConf().setAppName(self.getconfig(f"{C.SPARK}.{C.NAME}"))

        # This should not have much effect in Python, since after the the data is pickled, its passed as bytes array to Java
        # Kryo is a Java serializer. But might be worth to just turn it on to see if there's a noticeable difference.
        # conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        # Currently we don't (or almost don't) use Spark SQL, so this is probably irrelevant
        conf.set("spark.sql.execution.arrow.enabled", "true")

        sc = SparkContext.getOrCreate(conf=conf)
        sc.setLogLevel(self.getconfig(f"{C.SPARK}.{C.LOGLEVEL}"))

        self.dir4sparkzip = tempfile.mkdtemp()
        ship_files2spark(SparkSession(sc), self.dir4sparkzip, subdirs=('programs', 'hdmm/src/hdmm', 'das_framework', 'otherconsts'))
        
        # if sc.getConf().get("{}.{}".format(C.SPARK, C.MASTER)) == "yarn":
        #     # see stackoverflow.com/questions/36461054
        #     # --py-files sends zip file to workers but does not add it to the pythonpath.
        #     try:
        #         sc.addPyFile(os.environ[C.ZIPFILE])
        #     except KeyError:
        #         logging.info("Run script did not set environment variable 'zipfile'.")
        #         exit(1)

        # return SparkSession(sc)
        return self


class setup(DASDecennialSetup):
    """ For compatibility with existing config files"""
    pass
