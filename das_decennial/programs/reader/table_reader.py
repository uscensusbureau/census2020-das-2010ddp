"""
    This is the reader module for the DAS-Decennial instance of the DAS-framework.
    It contains a reader class that is a subclass of AbstractDASReader.
    The class must contain a method called read.
    It also defines an AbstractDASTable class for tracking metadata.
"""
from typing import Dict, List, Tuple, Union

import os
import os.path
import logging
import warnings
from collections import defaultdict
from configparser import NoOptionError

import numpy as np
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql import SparkSession

from das_framework.driver import AbstractDASReader, AbstractDASModule
import programs.nodes.nodes as nodes
import programs.sparse as sparse
# import programs.dashboard as dashboard
from programs.das_setup import DASDecennialSetup
from programs.das_rdd import DASDataFrame
from programs.rdd_like_list import RDDLikeList
import das_utils
import re

from das_framework.ctools.exceptions import Error, DASConfigError
from programs.exceptions import NodeRDDValidationError

import constants as C
#from constants import PARTITION_BY_BLOCK_GROUP, READER, TABLES, TABLE_CLASS, LEGAL, VAR_TYPE, RECODE_VARS, \
#    VARS, PATH, HEADER, DELIMITER, NUM_READER_PARTITIONS, PRE_RECODER, CTABLES, PTABLE, GEODICT_LENGTHS, GEODICT, \
#    RANGE_PARTITION, READER_PARTITION_LEN, REGEX_CONFIG_DELIM, MEASURE_RDD_TIMES, GEOGRAPHY, HISTOGRAM


class Interval(tuple):
    """
        Description:
            This class does the following:
                It creates a custom tuple for closed interval.
                Left and right endpoints included.
                Overloads contains the method.
                The tuple must be of length 2.
                Constructs a new Interval with the NewRange method.
    """
    @staticmethod
    def NewRange(left, right):
        """
            Description:
                This method creates an interval object.

            Inputs:
                left (and also right): They must be either an integer,
                                       a string representation of integer,
                                       or a single character as a string.
                Note: left and right must each be the same type.

            Output:
                Interval object
        """
        assert left <= right
        assert isinstance(left, type(right))
        assert isinstance(right, type(left))
        return Interval((left, right))

    def __init__(self, tup):
        """
            This will assert the length of the tuple is exactly 2.
        """
        assert len(tup) == 2
        self.left, self.right = tup

    def __contains__(self, item):
        """
            This checks if something is in a specific interval.
        """
        # TODO: "1BC" in Interval(("000","255")) returns true but we might want false.
        # some legal values include a mix of alphanumeric characters so maybe leave it as is.
        try:
            tmp = str(item)
        except TypeError:
            return False
        try:
            length_check = len(self.left) <= len(item) <= len(self.right)
        except TypeError:
            length_check = True
        return True if (self.left <= item <= self.right) and length_check else False

    def __len__(self):
        """
            This returns the length of an interval.
        """
        try:
            return ord(self.right) - ord(self.left) + 1
        except TypeError:
            return int(self.right) - int(self.left) + 1


class LegalList(list):
    """
        This is a subclass of list. It is intended to hold only Interval objects.
    """
    def __contains__(self, item):
        for interval in self:
            if item in interval:
                return True
        return False

    def __len__(self):
        return sum([len(interval) for interval in self])

    def __min__(self):
        return min([interval.left for interval in self])


class TableVariable:
    """
        This is a Variable metadata holder.
        It stores information about each variable in the table, including:
        type, SQL type, legal values, and 2018 end-to-end specific values.
    """
    # pylint: disable=invalid-name
    def __init__(self, name, vtype=None, legal=None):
        self.name = name
        self.vtype = vtype
        self.sql_type = self.get_sql_type()
        self.size = None
        self.end_to_end_value = None  # value for 2018
        self.legal_values = legal

    def make_from_config(self, config):
        """
            This will set the attributes reading from the config file.
        """
        try:
            vtype = config.get(C.READER, f"{self.name}.{C.VAR_TYPE}")
            legal_values = config.get(C.READER, f"{self.name}.{C.LEGAL}")
        except NoOptionError as e:
            raise DASConfigError(f"Missing variable {self.name} specifications", *e.args)

        self.set_vtype(vtype)
        self.set_legal_values(legal_values)

        return self

    def __str__(self):
        return self.name

    def __repr__(self):
        """
            This will return the representation of a Variable.
        """
        return "TableVariable(name:{} vtype:{})".format(self.name, self.vtype)

    def set_vtype(self, var_type):
        """
            This defines the string representation of a variable type.

            Input:
                var_type: supports "str" or "int"
        """
        assert var_type in ["int", "str"]
        self.vtype = var_type
        self.sql_type = self.get_sql_type()  # Keeps types in sync.

    def get_sql_type(self):
        """
            This returns the SparkSQL type of a variable.
        """
        return IntegerType() if self.vtype == "int" else\
               StringType() if self.vtype == "str" else None

    def set_legal_values(self, legal_string):
        """
            This sets the "legal values."

            Input:
                legal_string: a string of one of the following forms:
                    3
                    2,3,4
                    2-4
                    3-8,4,5,9-12,
                    etc
        """
        ranges = legal_string.strip().split(",")
        legal = []
        for split_string in ranges:
            endpoints = split_string.strip().split("-")
            left = endpoints[0].strip()
            right = endpoints[-1].strip()
            if self.sql_type == IntegerType():
                legal.append(Interval.NewRange(int(left), int(right)))
            else:
                legal.append(Interval.NewRange(left, right))
        self.legal_values = LegalList(legal)

    def __eq__(self, other):
        return self.name == other.name


class AbstractTable(AbstractDASModule):
    """
        This class is a support object for storing table layout information.
    """
    reader: 'reader'  # Pointer to the reader instance, to get the variables that are common for all read tables
    location: str  # Location of the file with data for the table (usually, CEF as CSV)
    variables: List[TableVariable]  # List of variables
    recode_variables: List[TableVariable]  # List of recodes to make
    csv_file_format: Dict[str, Union[bool, str, StructType]]  # Options for reading the CSV file
    geography_variables: Tuple[str, ...]  # List of variables defining geography
    histogram_variables: Tuple[str, ...]  # List of variables on which the histogram is constructed
    data_shape: Tuple[int, ...]  # Shape of the histogram
    
    # pylint: disable=invalid-name
    def __init__(self, *, reader_instance: 'reader', **kwargs):
        super().__init__(**kwargs)

        self.reader = reader_instance

        self.location = [os.path.expandvars(x) for x in re.split(C.REGEX_CONFIG_DELIM, self.getconfigwsec(C.PATH))]

        self.variables = [TableVariable(var_name).make_from_config(self.config) for var_name in self.gettuplewsec(C.VARS)]

        self.recode_variables = [TableVariable(var_name).make_from_config(self.config) for var_name in self.gettuplewsec(C.RECODE_VARS, default=())]
        
        self.csv_file_format = self.reader.csv_file_format.copy()

        # If we want these distinct for each table, then the option should include table name
        # self.csv_file_format = {
        #     "header": self.reader.csv_file_format["header"]
        #     "sep": self.reader.csv_file_format["sep"]
        # }

        self.csv_file_format['schema'] = self.set_schema()

        self.geography_variables = self.gettuplewsec(C.GEOGRAPHY)
        self.histogram_variables = self.gettuplewsec(C.HISTOGRAM)

        # Finally, set up the recoder, if there is any. This must be done in __init__() so the recoder is included in the BOM.

        recoder_name = f"{self.name}.{C.PRE_RECODER}"
        if not self.config.has_option(C.READER, recoder_name):
            self.recoder=None
        else:
            args = [self.gettuple(var.name, section=C.READER, sep=" ") for var in self.recode_variables]
            if self.getboolean(f"{self.name}.{C.NEWRECODER}", section=C.READER, default=False):
                args = args + [self.recode_variables]
            try:
                self.recoder = das_utils.class_from_config(self.config, recoder_name, C.READER)(*args)
            except TypeError as err:
                raise TypeError(f"Table {self.name} failed to create recoder, arguments: {args}, Error: {err.args}")


    def getconfigwsec(self, key):
        """
        Get config option from the config file, from READER section
        :param key: option name
        :return: str value of config option
        """
        return super().getconfig(f"{self.name}.{key}", section=C.READER)

    def gettuplewsec(self, key, default=None):
        """
        Get config option from the config file, from READER section, and parse as a tuple
        :param key: option name
        :param default: required by super().gettuple
        :return: tuple of str, listed in config option
        """
        return super().gettuple(f"{self.name}.{key}", default=default, section=C.READER, sep=" ")

    def add_variable(self, new_var):
        """
            This adds/stores information for a new variable to a self object.

            Input:
                new_var: a TableVariable object
        """
        assert isinstance(new_var, TableVariable)
        if new_var in self.variables:
            raise Error(f"Trying to add TableVariable {new_var} which is already in table {self.name} (variables: {self.variables})")
        self.variables.append(new_var)
        # The following line is not needed, since add_variable is called either after reading the csv,
        # or, if before, it adds recode variables which don't exist in csv and thus not needed in the schema
        # self.csv_file_format["schema"] = self.set_schema()

    def get_variable(self, name):
        """
            This will get the name of a variable, if possible.

            Input:
                name: the name of variable to return

            Output: the variable with name "name" or not if no such variable
        """
        for var in self.variables:
            if var.name == name:
                return var
        return None

    def set_schema(self):
        """
            This will return the SQL schema/structure type for a self object.
        """
        return StructType([StructField(v.name, v.sql_type) for v in self.variables])

    def set_shape(self):
        """ Call after recodes to ensure vars have all been added to table """
        self.data_shape = tuple([len(self.get_variable(var).legal_values) for var in self.histogram_variables])

    def load(self):
        """
            This loads the records from a csv into a Spark dataframe.

            Input:
                spark: the SparkSession object

            Output: the Spark dataframe object
        """
        spark = SparkSession.builder.getOrCreate()
        return spark.read.csv(self.location, **self.csv_file_format)

    def recode_meta_update(self):
        """
            This adds the recode variables to the list of table variables.
        """
        try:
            for v in self.recode_variables:
                self.add_variable(v)
        except TypeError:
            pass

    def pre_recode(self, data):
        """
            This applies predisclosure avoidance recodes.

            Inputs:
                data: a Spark dataframe (df)
                config: a ConfigParser where the recodes are specified

            Output:
                a Spark dataframe (df) with the recode columns added
        """

        # If recoder is not set, just return the data as is
        if self.recoder is not None:
            return data.rdd.map(self.recoder.recode).toDF()
        else:
            return data

    def filter(self, data, test_area):
        """ When using a test area, filter the RDD by removing geocodes outside the test area"""
        if test_area != "":
            data = data.rdd.filter(lambda row: "".join([str(row[code]) for code in self.geography_variables]).startswith(test_area)).toDF()
        return data

    def process(self, data_structure):
        """
            This function processes raw input, which must be implemented in child classes.

            Inputs:
                data_structure: a data object (eg: spark, df, or ordd)

            Output:
                defaults to the identity function
        """
        return data_structure

    def verify(self, data):
        """
            This is a quick pass over the data to ensure that all values are as expected.

            Input:
                data - spark df
            Output:
                Either:
                (a) "True" if data is valid,
                otherwise,
                (b) an assertion error.
        """
        return data.rdd.filter(lambda row: self.check_row(row) == 0).isEmpty()

    def check_row(self, row):
        """
            Input:
                row: the row of the Spark dataframe

            Output:
                Either:
                (a) "True" if row is valid,
                otherwise,
                (b) an assertion error.
        """
        for var in self.variables:
            assert var.name in row, "{} is not in row: {}".format(var.name, row)
            assert row[var.name] in var.legal_values, "{} is not a legal value for {}, the bad row was {}".format(
                row[var.name], var.name, row)
        return True


class DASDecennialReader(AbstractDASReader):
    """
        The CEF reader object loads microdata and metadata into tables
        and then converts them into a usable form.
    """

    setup: DASDecennialSetup
    invar_names: List[str]  # Names of bottom (Block) level invariants
    cons_names: List[str]   # Names of bottom (Block) level constraints
    constraint_table_name: str
    constraint_tables: Tuple[str, ...]
    privacy_table_name: str
    data_names: List[str]
    geocode_dict: dict

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        assert self.config

        self.csv_file_format = {
            "header": self.getboolean(C.HEADER),
            "sep": self.getconfig(C.DELIMITER),
            "comment": self.getconfig(C.CSV_COMMENT, default='#')
        }

        self.num_reader_partitions = self.getint(C.NUM_READER_PARTITIONS, default=100)
        self.range_partition = self.getboolean(C.RANGE_PARTITION, default=False)
        self.reader_partition_len = self.getint(C.READER_PARTITION_LEN, default=11)
        self.measure_rdd_times = self.getboolean(C.MEASURE_RDD_TIMES, default=False)

        table_names = self.gettuple(C.TABLES, sep=" ")

        logging.info("building table infrastructure")
        logging.debug("table names %s", table_names)
        logging.debug("Reading table module and class names from config")

        # Create the reader classes for each table.
        # They are subclasses of the AbstractDASModule, so they need to be properly setup
        self.tables = {name: das_utils.class_from_config(self.config, f"{name}.{C.TABLE_CLASS}", C.READER)(
                           name=name, config=self.config, reader_instance=self, das=self.das) 
                       for name in table_names}

        self.shape_dict = {}

        # Find out recode variables and their dimensions to know the set of histogram variables and its shape
        for table in self.tables.values():
            logging.info(f"recode meta for table {table.name}")
            table.recode_meta_update()
            table.set_shape()
            self.shape_dict[table.name] = table.data_shape

        # Bottom geographical level
        bottom: str = self.setup.levels[0]

        # Invariants and Constraints for the bottom level
        ic_bottom: Dict[str, List[str]] = self.setup.inv_con_by_level[bottom]

        # Get invariants names from setup
        self.invar_names = ic_bottom["invar_names"]

        # Get constraints names from setup
        self.cons_names = ic_bottom["cons_names"]

        # Get the names of tables
        self.privacy_table_name = self.getconfig(C.PTABLE).strip()  # Person or Household table
        self.constraint_tables = self.gettuple(C.CTABLES)           # it allows for extra-generality, but we only need one additional table for invariants/constaints, i.e.
        self.constraint_table_name = self.constraint_tables[0]    # Unit table
        self.data_names = [self.privacy_table_name] + list(self.constraint_tables)

        # Shape of the person histogram (save it in setup object for further use)
        if self.setup.hist_shape != self.tables[self.privacy_table_name].data_shape:
            msg = (f"The histogram shape set in config file {self.tables[self.privacy_table_name].data_shape} that the data read is different from " 
                   f"the shape of schema {self.setup.schema} {self.setup.hist_shape}")
            warnings.warn(msg)
            self.log_warning_and_print(msg)
            self.setup.hist_shape = self.tables[self.privacy_table_name].data_shape

        # Save person tables histogram variables in setup
        if self.setup.hist_vars != self.tables[self.privacy_table_name].histogram_variables:
            # msg = f"The histogram variables set in config file {self.tables[self.privacy_table_name].histogram_variables} that the data read are " \
            #     f"different from the variables of schema {self.setup.schema} {self.setup.hist_vars}"
            # warnings.warn(msg)
            # self.log_warning_and_print(msg)
            self.setup.hist_vars = self.tables[self.privacy_table_name].histogram_variables

        # Create geocode dict
        geolevel_leng = self.gettuple(C.GEODICT_LENGTHS, section=C.GEODICT)
        assert len(geolevel_leng) == len(self.setup.levels), "Geolevel names and geolevel lengths differ in size"
        self.geocode_dict = {int(gl_length): gl_name for gl_name, gl_length in zip(self.setup.levels, geolevel_leng)}

    def read(self):
        """
            This function performs the following steps:

            (1) Load input file into spark dataframe.
            (2) Convert dataframe into a rdd of ndarrays
                (one ndarray for each block).
        """

        if self.setup.postprocess_only:
            return None

        # Filter to test area
        try:
            test_area = self.getconfig("test_area")
        except NoOptionError:
            test_area = ""

        logging.info("loading the data")
        tmp = defaultdict()
        for table in self.tables.values():
            logging.info(f"loading table {table.name}")
            table_df = table.load()
            # if not table.verify(table_df):
            #    print("table contains invalid records")
            #    raise RuntimeError
            logging.info(f"applying filter to {table.name}")
            table_df = table.filter(table_df, test_area)

            logging.info(f"recodes for {table.name}")
            table_df = table.pre_recode(table_df)
            if self.measure_rdd_times:
                table_df = DASDataFrame(table_df)
            logging.info("dict")
            # TODO: Why do we need a tuple? Why not put just the processed table?
            if table:
                tmp[table.name] = (table, table.process(table_df))

            # print(tmp[table.name][1].count())
            # raise RuntimeError

        logging.info("processing the input")
        table_df_dict = tmp

        person_data = table_df_dict[self.privacy_table_name][1]
        unit_data = table_df_dict[self.constraint_table_name][1]

        join_data = person_data.rightOuterJoin(unit_data)
        # print(f"(geocode, hists)", das_utils.rddPartitionDistributionMoments(join_data))

        # make block nodes
        # from programs.rdd_like_list import RDDLikeList
        # block_nodes = RDDLikeList(join_data.collect()).map(self.makeBlockNode)
        block_nodes = join_data.map(self.makeBlockNode)

        # If not using spark in engine, convert into RDDLikeList
        if not self.setup.use_spark:
            block_nodes = RDDLikeList(block_nodes.collect())

        # print(f"Block nodes", das_utils.rddPartitionDistributionMoments(block_nodes))

        num_partitions = self.num_reader_partitions
        if self.getboolean(C.PARTITION_BY_BLOCK_GROUP, default=False):
            block_nodes = das_utils.partitionByParentGeocode(block_nodes, num_partitions)
        elif abs(block_nodes.getNumPartitions() - num_partitions)/num_partitions > 0.2:
            # This is for comparison with just changing the number of partitions
            print("Repartitioning with default hash partitioner...")
            block_nodes = block_nodes.repartition(num_partitions)
            # block_nodes = das_utils.partitionBySiblingNumber(block_nodes, num_partitions)

        # print(f"Block nodes (repartitioned)", das_utils.rddPartitionDistributionMoments(block_nodes))

        block_nodes.persist()
        join_data.unpersist()

        # Check that the input data satisfies constraints
        if self.setup.validate_input_data_constraints:
            self.validateConstraintsNodeRDD(block_nodes, self.setup.levels[0])

        return block_nodes

    @staticmethod
    def validateNodeRDD(node_rdd, failed_node_function=lambda d: False, failed_msg="failed", sample_description="Geocodes", sample_function=lambda node: node.geocode, error=False, sample_size=5):
        """
        Generic function to validate each GeounitNode in an RDD.
        1) Checks each element of :node_rdd: with :failed_node_function:
        2) Checks whether there are elements that failed validation
        3) If yes, issues warning
        4) Prints out a sample of failed nodes, with each node printed by :sample_function:

        This is only staticmethod of the reader module, because reader module is where such validation would typically take place.
        Since it's static, it can be easily taken out in the future

        :param node_rdd: RDD of GeounitNodes to validate
        :param failed_node_function: validating function, returns True if node fails and False if node passes validation
        :param failed_msg: Diagnostic message describing what "failed validation" is (e.g. "failed constraints", "lack invariants" etc.)
        :param sample_description: Description for string returned for each of the sampled failed nodes
        :param sample_function: Function converting a failed node to a string describing it
        :param error: if True throw Exception, otherwise throw warning
        :param sample_size: How many failed nodes to prints
        """
        failed_nodes = node_rdd.filter(failed_node_function)
        fn_count = failed_nodes.count()
        if fn_count > 0:
            failed_node_sample = failed_nodes.take(sample_size)

            msg = f"{fn_count} nodes {failed_msg}"

            sample_msg = f"Sample of {sample_size} failed nodes ({failed_msg}):\n" \
                f"{sample_description}:"

            err = NodeRDDValidationError(msg, sample_msg, tuple(map(sample_function, failed_node_sample)))

            if error:
                raise err
            else:
                logging.warning(err.msg)
                warnings.warn(err.msg)
                print(f"{err.sample_msg} {err.sample}")

        das_utils.freeMemRDD(failed_nodes)

    def validateConstraintsNodeRDD(self, node_rdd, level):
        """
        Make sure that every node in RDD satisfies its own constraints
        :param node_rdd:
        :param level: which geolevel (for diagnostic messaging)
        """
        self.log_and_print(f"Checking if all of the {level} constraints are satisfied on input data")
        self.annotate(f"validateNodeRDD {level} starting")
        self.validateNodeRDD(node_rdd,
                             failed_node_function=lambda node: not node.checkConstraints(),
                             failed_msg=f"in input data ({level}) don't satisfy constraints",
                             sample_description="Failed constraints (!!!POSSIBLY TITLED INFO!!!) as (geocode, [(constraint_name, RHS-in-actual-data, RHS-as-set-by-constraint, sign)...])",
                             sample_function=lambda node: (node.geocode, node.checkConstraints(return_list=True)),
                             error=False)
        self.annotate(f"validateNodeRDD {level} finished")

    def makeBlockNode(self, person_unit_arrays):
        """
            This function makes block nodes from person unit arrays for a given geocode.

            Inputs:
                config: a configuration object
                person_unit_arrays: a RDD of (geocode, arrays), where arrays are the tables defined in the config

            Output:
                block_node: a nodes.GeounitNode object for the given geocode
        """

        geocode, arrays = person_unit_arrays

        # Assign arrays to table names in a dictionary {name:array} and fill in with zeros if array is non-existent
        assert len(arrays) == len(self.data_names)
        data_dict = {n: a
                        if a is not None
                        else np.zeros(self.shape_dict[n]).astype(int)  # TODO: Wonder if this creation of zeros takes too much time, maybe directly in multisparse?
                     for n, a in zip(self.data_names, arrays)}

        # geocode is a tuple where the [1] entry is empty. We only want the [0] entry.
        geocode = geocode[0]
        logging.info(f"creating geocode: {geocode}")

        raw = sparse.multiSparse(data_dict[self.privacy_table_name], shape=self.shape_dict[self.privacy_table_name])
        raw_housing = sparse.multiSparse(data_dict[self.constraint_table_name], shape=self.shape_dict[self.constraint_table_name])

        # Make Invariants
        invariants_dict = self.setup.makeInvariants(raw=raw, raw_housing=raw_housing, invariant_names=self.invar_names)

        # Make Constraints
        constraints_dict = self.setup.makeConstraints(hist_shape=self.setup.hist_shape, invariants=invariants_dict, constraint_names=self.cons_names)

        block_node = nodes.GeounitNode(geocode=geocode, geocode_dict=self.geocode_dict,
                                       raw=raw, raw_housing=raw_housing,
                                       cons=constraints_dict, invar=invariants_dict)
        return block_node
