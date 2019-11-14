"""
engine_utils.py:
Contains:
DASEngineHierarchical - parent class for engines, containing functions shared between engines
    including the actual implementation of the top-down algorithm
The actual calls to gurobi appear in nodes.manipulate_nodes, which is called in a .map
(And which runs on the worker nodes).
"""

import logging
import os
import os.path
from abc import ABCMeta, abstractmethod
from operator import add
from typing import Tuple, List, Dict, Union, Iterable
from configparser import NoOptionError, NoSectionError
import json
import pickle

import numpy as np

from pyspark.rdd import RDD
from pyspark.sql import SparkSession

from das_framework.driver import AbstractDASEngine, DAS
import das_framework.ctools.clogging as clogging
from das_framework.ctools.exceptions import DASConfigError

import programs.engine.primitives as primitives
import programs.queries.querybase as querybase
import programs.queries.constraints_dpqueries as cons_dpq
import programs.queries.make_queries as make_queries
import programs.s3cat as s3cat

from programs.das_setup import DASDecennialSetup
from programs.nodes.nodes import GeounitNode
from programs.rdd_like_list import RDDLikeList

import das_utils

import constants as C
from constants import CC

__NodeRDD__ = RDD
__NodeDict__ = Dict[str, __NodeRDD__]
__Nodes__ = Union[__NodeRDD__, __NodeDict__]


class DASEngineHierarchical(AbstractDASEngine, metaclass=ABCMeta):
    """
    Parent class for engines, containing functions shared between engines.
    For engines that infuse noise at each geolevel, setPerGeolevelBudgets() sets the budgets
    For engines that use predefined (in config) queries setPerQueryBudgets() set the budget distribution between those and the detailed query
    For engines using topdown optimization, topdown function is implemented
    noisyAnswers is implemented here only for topdown engines
    """

    # pylint: disable=too-many-instance-attributes
    setup: DASDecennialSetup                                 # Setup module/object of the DAS
    minimal_schema: Tuple[str, ...]                          # vars in minimal schema (for minimal schema engine)
    vars_schema: Dict[str, int]                              # vars in the full schema
    hist_shape: Tuple[int, ...]                              # Shape of the data histogram
    total_budget: float                                      # Total budget

    geolevel_prop_budgets: Tuple[float, ...]                 # Shares of budget designated to each geolevel
    detailed_prop: float                                     # Share of within-geolevel budgets dedicated to detailed histogram
    dp_query_prop: Tuple[float, ...]                         # Shares of within-geolevel budgets dedicated to each query
    dp_query_names: List[str]                                # Queries by name
    dp_queries: bool                                         # Whether there are any queries (in addition to detailed histogram)
    queries_dict: Dict[str, querybase.AbstractLinearQuery]   # Dictionary with actual query objects

    saveloc: str                                             # Where to save noisy measurements

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # Person histogram shape to get later from setup object, once reader is initiated and know table parameters
        self.hist_shape = self.setup.hist_shape

        # pylint: disable=bad-whitespace
        self.levels: Tuple[str, ...] = self.setup.levels
        self.levels_reversed: Tuple[str, ...] = tuple(reversed(self.levels))
        self.schema: str = self.setup.schema
        logging.info(f"levels: {self.levels}")

        self.vars_schema = {var: i for i, var in enumerate(self.setup.hist_vars)}
        self.log_and_print(f"the variables in the schema are: {self.vars_schema} (reader names)")
        try:
            self.log_and_print(f"Their names in the {self.schema} schema are {self.setup.schema_obj.dimnames}")
        except AssertionError:
            self.log_warning_and_print(f"Schema {self.schema} is not supported")

        # Default is False. It has to be set for minimal schema engine, but in regular topdown it is used too, for fail safe
        self.minimal_schema = self.gettuple(C.MINIMALSCHEMA, section=C.CONSTRAINTS, sep=" ", default=False)

        # Whether to run in spark (there is a local/serial mode, for testing and debugging)
        self.use_spark = self.setup.use_spark

        # Whether to save noisy answers
        self.save_noisy: bool   = self.getboolean(C.SAVENOISY, default=True) if self.use_spark else False

        # Whether to discard noisy answers and reload them (makes sure
        # the noise is only drawn once and not again during RDD
        # reconstruction)

        self.reload_noisy: bool = self.getboolean(C.RELOADNOISY, default=True) if self.save_noisy else False

        # For restarting the DAS from the point after the noise was
        # generated and saved, so that no additional budget is spent
        self.postprocess_only: bool = self.setup.postprocess_only

        if self.postprocess_only:
            # Unique run ID of the run where the noisy answers were saved, to load for postprocess-only
            self.saved_noisy_app_id: str = self.getconfig(C.SAVED_NOISY_APP_ID)

        if self.das.make_bom_only():
            self.app_id = C.APPID_NO_SPARK
        else:
            self.app_id: str = clogging.applicationId()  # if self.use_spark else C.APPID_NO_SPARK

        # Statistics System. Indicate which statistics we should save or not save.
        self.record_gurobi_stats: bool = self.getboolean(C.RECORD_GUROBI_STATS_OPTION, section=C.GUROBI_SECTION, default=False)
        self.record_cpu_stats: bool    = self.getboolean(C.RECORD_CPU_STATS_OPTION,    section=C.GUROBI_SECTION, default=False)
        self.record_vm_stats: bool     = self.getboolean(C.RECORD_VM_STATS_OPTION,     section=C.GUROBI_SECTION, default=False)
        self.save_gurobi_stats: bool   = self.getboolean(C.SAVE_STATS_OPTION,          section=C.GUROBI_SECTION, default=False)

        # Privacy protection mechanism (see primitives.py). Geometric is default, set explicitly in subclasses
        self.mechanism = primitives.basic_dp_answer

        # Shares of budget designated to each geolevel. Should be None if not doing by-level bugdet (e.g.bottomup engine)
        self.geolevel_prop_budgets: tuple = None

    def initializeAndCheckParameters(self):
        """
        The things done here should conceptually be done in the __init__, but since the experiment regime in
        das_framework skips the setup part for the experimental runs expect initial ones, this function has to be
        called to reset the parameters to the values set in the new config corresponding to the new experimental run
        :return:
        """
        self.total_budget: float = self.getfloat(C.EPSILON_BUDGET_TOTAL, section=C.BUDGET)
        self.log_and_print(f"The total budget is: {self.total_budget}")

        # sensitivity specification (global / for detailed histogram)
        # self.sens: float = self.getfloat(C.BOUNDED_DP_MULTIPLIER, section=C.BUDGET, default=C.DEFAULT_BOUNDED_DP_MULTIPLIER)

        # Where to save noisy answers (different folder for each experiment, so here) or load from for postprocessing only
        if self.save_noisy or self.postprocess_only:
            self.saveloc: str = os.path.join(self.getconfig(C.OUTPUT_PATH, section=C.WRITER, expandvars=True),
                                             self.getconfig(C.NOISY_MEASUREMENTS_POSTFIX, default="noisy_measurements"))

    def setPerGeolevelBudgets(self):
        """
        For engines infusing noise at each geolevel (e.g. topdown, minimal_schema, hdmm*)
        Read and set the by-geolevel privacy budget distribution, check that it sums to 1
        :return:
        """
        # Fractions of how the total <engine> epsilon is split between geolevels
        self.geolevel_prop_budgets: tuple = self.gettuple_of_floats(C.GEOLEVEL_BUDGET_PROP, section=C.BUDGET, sep=C.REGEX_CONFIG_DELIM)
        self.log_and_print(f"The budget is split between geolevels with proportions: {self.geolevel_prop_budgets}")

        # check that geolevel_budget_prop adds to 1, if not raise exception
        assertSumTo(self.geolevel_prop_budgets, msg="Across-geolevel Budget Proportion")

    def setPerQueryBudgets(self):
        """
        For engines with queries set in config (e.g. topdown, minimal_schema, bottomup)
        Read the queries from config, and set their budget allocations. Check that allocation proportions sum to one/
        :return:
        """
        # TODO: Remove separate treatment of the detailed query and other dp_queries, here and in makeDPNode (then also remove .dp setter in nodes.py)
        # Fraction of per-geolevel budget dedicated to the full histogram
        try:
            self.detailed_prop: float = self.getfloat(C.DETAILEDPROP, section=C.BUDGET)
        except (NoSectionError, NoOptionError):
            self.detailed_prop = False

        # Make a list to check later if it sums up to 1.
        budget_per_each_query: list = [float(self.detailed_prop)]

        try:
            self.dp_query_names = list(self.gettuple(C.DPQUERIES, section=C.BUDGET, sep=C.REGEX_CONFIG_DELIM))  # note this must be a list
            self.dp_queries = True
        except (NoOptionError, NoSectionError):
            self.dp_queries = False

        try:
            # Try to load small-cell-queries determined by SparkSQL analysis of SF1 2000 public tabulations
            self.small_cell_basepath = self.getconfig(C.SMALLCELLBASEPATH, section=C.BUDGET)
            self.small_cell_geolevels = self.gettuple(C.SMALLCELLGEOLEVELS, section=C.BUDGET)
            self.small_cell_query = True
        except (NoOptionError, NoSectionError):
            self.small_cell_query = False

        budgetSplitLogMsg = f"The sequential-composition budget allocation between queries in a geolevel is:"
        budgetSplitLogMsg += f"\nDetailed Queries Budget: "
        budgetSplitLogMsg += (f"{self.detailed_prop}" if self.detailed_prop else "1.0")

        if not (self.detailed_prop or self.dp_queries):
            msg = f"For manual workload setup, either [{C.ENGINE}]/{C.DETAILEDPROP} or [{C.ENGINE}]/{C.DPQUERIES} have to be set, but both are empty."
            raise DASConfigError(msg, section=C.ENGINE, option=f"{C.DETAILEDPROP} nor {C.DPQUERIES}")

        if self.dp_queries:
            self.dp_query_prop = self.gettuple_of_floats(C.QUERIESPROP, section=C.BUDGET, sep=C.REGEX_CONFIG_DELIM)
            print(f"DPquery names: {self.dp_query_names}")
            self.queries_dict = make_queries.QueriesCreator(self.setup.schema_obj, self.dp_query_names).queries_dict

            # Add the fractions of per-geolevel budgets dedicated to each query to the list that should sum up to 1.
            budget_per_each_query.extend(list(self.dp_query_prop))
            budgetSplitLogMsg += f"\nDPQueries: {self.dp_query_prop}"
        
        if self.small_cell_query:
            self.small_cell_prop = self.getfloat(C.SMALLCELLPROP, section=C.BUDGET)
            print(f"Small cell geolevels requested in config: {self.small_cell_geolevels}")

            # Add public-historica-data small-cell-query
            budget_per_each_query.extend([self.small_cell_prop])
            budgetSplitLogMsg += f"\nsmallCellQuery: {self.small_cell_prop}"
            
        self.log_and_print(budgetSplitLogMsg)
        # check that budget proportion adds to 1, if not raise exception
        assertSumTo(budget_per_each_query, msg="Within-geolevel Budget Proportion")

    @abstractmethod
    def run(self, original_data: __NodeRDD__) -> None:
        """
        Run the engine.
        The prototype does not do anything to the data, just sets up engine configuration
        :param original_data: lowest (block) level Geounit nodes RDD
        :return: unchanged original data (privatization of data is implemented in subclasses)
        """
        self.annotate(f"{self.__class__.__name__} run")

        # initialize parameters
        self.initializeAndCheckParameters()

        # print the histogram shape
        print(f"Histogram shape is: {self.hist_shape}")

        pass

    def writeGurobiStatistics(self, all_nodes: __NodeRDD__) -> None:
        """@param all_nodes - an RDD containing all of the nodes.
        Writes gurobi statistics from nodes to S3 and then records the fact
        that they were written in the DFXML file. Called below from
        topdown()"""

        num_partitions = self.getint(C.STATISTICS_PARTITIONS, section=C.GUROBI_SECTION, default=C.MAX_STATISTICS_PARTITIONS)
        self.annotate(f"coalescing statistics to {num_partitions} partitions and then dumping with json.dumps", verbose=True)
        all_stats = all_nodes.flatMap(lambda node: node.gurobi_stats).coalesce(num_partitions).map(json.dumps)
        # TODO: Maybe remove coalesce
        # all_stats = all_nodes.flatMap(lambda node: node.gurobi_stats).map(json.dumps)
        self.annotate(f"Total Gurobi statistics: {all_stats.count()}", verbose=True)
        ofn = self.getconfig(C.GUROBI_STATISTICS_FILENAME,
                             section=C.GUROBI,
                             default=C.DEFAULT_GUROBI_STATISTICS_FILENAME,
                             expandvars=True)
        all_stats.saveAsTextFile(ofn)
        DAS.instance.dfxml_writer.add_application_kva('gurobiStatistics', attr={'filename': ofn})
        self.log_and_print(f"Gurobi statistics URL: {ofn}")
        json_file = s3cat.s3cat(ofn, demand_success=True, suffix='.json')
        self.log_and_print(f"Combined Gurobi statistics URL: {json_file}")

    def getNodesAllGeolevels(self, block_nodes: __NodeRDD__, additional_ic: bool = True) -> __NodeDict__:
        """
        This function takes the block_nodes RDD and aggregates them into higher level nodes.GeounitNode RDD objects:

        Inputs:
            block_nodes: RDDs of GeounitNode objects aggregated at the block level
            additional_ic: For bottom up engine, it is not needed to deal with constraints, so can be set to False

        Outputs:
            by-level dictionary of nodes
        """
        nodes_dict: __NodeDict__ = {self.levels[0]: block_nodes}

        for level, upper_level in zip(self.levels[:-1], self.levels[1:]):
            nodes_dict[upper_level] = nodes_dict[level] \
                                        .map(lambda node: (node.parentGeocode, node)) \
                                        .reduceByKey(lambda x, y: x.addInReduce(y)) \
                                        .map(lambda d: (lambda geocode, node: node.shiftGeocodesUp())(*d))

            if additional_ic:
                nodes_dict[upper_level] = \
                    nodes_dict[upper_level].map(lambda node: node.makeAdditionalInvariantsConstraints(self.setup))
            nodes_dict[upper_level].persist()

            # Check that newly made level RDD of nodes satisfies constraints
            # TODO: Decide how default we want this, and where the function should reside
            if self.setup.validate_input_data_constraints:
                DAS.instance.reader.validateConstraintsNodeRDD(nodes_dict[upper_level], upper_level)

        # garbage collect for memory
        self.freeMemRDD(block_nodes)

        return nodes_dict

    def makeOrLoadNoisy(self, nodes: __Nodes__) -> __Nodes__:
        """
        Obtain noisy nodes for postprocessing. If [engine]/POSTPROCESS_ONLY config option is True, then
        load the noisy nodes saved in the DAS run with application-id indicated in the config file
        [engine]/NOISY_APP_ID.
        Otherwise, generate noisy nodes (i.e. take noisy measurements)
        If SAVENOISY option is True, save the noisy nodes.
        If RELOADNOISY option is True, discard the noisy nodes from memory and load them from the saved files.
        :rtype: Nodes
        :param nodes: RDD with geoNodes without the noisy data of the bottom geographical levels or by-level dict of RDDs of nodes
        :return: by-geolevel dictionary of noisy nodes RDDs or a single RDD with nodes
        """
        if self.postprocess_only:
            self.log_and_print("Postprocessing only, loading noisy answers")
            return self.loadNoisyAnswers(self.saved_noisy_app_id)
        else:
            self.annotate("Generating noisy answers")

            if self.geolevel_prop_budgets is not None:
                # make higher level nodes by aggregation
                # getNodesAllGeolevels is defined in engine_utils().
                # It returns a dictionary of RDDs, one for each geolevel.
                # This causes the algorithm to run.
                #
                nodes: __NodeDict__ = self.getNodesAllGeolevels(nodes)

            # This functions return either a single RDD or a dictionary, depending on subclass
            nodes: __Nodes__ = self.noisyAnswers(nodes)
            nodes: __Nodes__ = self.deleteTrueData(nodes)

            # Save the noisy nodes if required. Delete them from memory and reload if required
            nodes: __Nodes__ = self.saveAndOrReload(nodes)

            return nodes

    @abstractmethod
    def noisyAnswers(self, nodes: __Nodes__, total_budget: float = None, dp_queries: bool = True) -> __Nodes__:
        """ Return noisy answers for nodes with true data"""
        pass

    @abstractmethod
    def freeMemNodes(self, nodes: __Nodes__) -> None:
        """ Unpersist and del nodes and collect the garbage"""
        pass

    def saveAndOrReload(self, nodes: __Nodes__) -> __Nodes__:
        """ Save the noisy nodes if required. Delete them from memory and reload if required """
        if self.save_noisy:
            self.annotate("Saving noisy answers")
            self.saveNoisyAnswers(nodes)

            if self.reload_noisy:
                self.freeMemNodes(nodes)

                self.annotate("Reloading noisy answers")
                return self.loadNoisyAnswers(self.app_id)

        return nodes

    def deleteTrueData(self, nodes: Union[__NodeRDD__, __NodeDict__]) -> __NodeRDD__:
        """
        Deletes true data from a single RDD of geoNodes (nodes.py) and checks that it's done
        :param nodes: RDD of geoNodes
        :return: RDD of geoNodes with 'raw' field equal to None
        """
        if self.getboolean(C.DELETERAW, default=True):
            logging.info("Removing True Data Geolevel Array")
            ###
            ## explicitly remove true data "raw"
            ##
            nodes = nodes.map(lambda node: node.deleteTrueArray()).persist()
            ##
            ## check that true data "raw" has been deleted from each, if not will raise an exception
            ##
            nodes.map(lambda node: node.checkTrueArrayIsDeleted())
        return nodes

    def saveNoisyAnswers(self, nodes: __NodeDict__, postfix: str = "") -> None:
        """
        Save RDDs with geonodes as pickle files, by geolevel
        :param nodes: RDD or by-geolevel dictionary of noisy nodes RDDs
        :param postfix: postfix to add to default filename (e.g. "_ms" for minimal_schema run)
        :return:
        """
        for level, nodes_rdd in nodes.items():
            path = self.noisyPath(self.app_id, level, postfix)
            das_utils.savePickledRDD(path, nodes_rdd)

        das_utils.saveConfigFile(os.path.join(self.saveloc, f"{self.app_id}-bylevel_pickled_rdds.config"), self.config)

    def loadNoisyAnswers(self, application_id: str, postfix: str = "") -> __NodeDict__:
        """
        Load pickled noisy geonodes for all levels
        :param application_id: Unique DAS run ID which is part of filenames to load
        :param postfix: postfix to add to default filename (e.g. "_ms" for minimal_schema run)
        :return: by-geolevel dictionary of noisy nodes RDDs
        """
        spark = SparkSession.builder.getOrCreate()

        # Only load bottom level if no budgets spent on higher levels (like in bottom-up)
        levels2load = self.levels if self.geolevel_prop_budgets is not None else self.levels[:1]

        nodes_dict = {}
        for level in levels2load:
            path = self.noisyPath(application_id, level, postfix)
            if path.startswith(C.HDFS_PREFIX):
                level_rdd = spark.sparkContext.pickleFile(das_utils.expandPathRemoveHdfs(path))
            elif das_utils.isS3Path(path):
                level_rdd = spark.sparkContext.pickleFile(path)
            else:
                level_rdd = spark.sparkContext.parallelize(pickle.load(path))
            nodes_dict[level] = level_rdd if self.use_spark else RDDLikeList(level_rdd.collect())

        return nodes_dict

    def noisyPath(self, application_id, level, postfix):
        """ Form the filename to save/load noisy measurements
        :param application_id: Unique DAS run ID which is part of filenames to load/save
        :param postfix: postfix to add to default filename (e.g. "_ms" for minimal_schema run)
        :param level: for which geolevel is the saved/loaded file
        """
        return os.path.join(self.saveloc, f"{application_id}-{level}{postfix}.pickle")

    @staticmethod
    def freeMemRDD(some_rdd: __NodeRDD__) -> None:
        """ Remove RDD from memory, with garbage collection"""
        das_utils.freeMemRDD(some_rdd)

    def freeMemLevelDict(self, nodes_dict: __NodeDict__, down2level=0) -> None:
        """Unpersist and remove RDDs for each geolevel from memory"""
        for level in self.levels[down2level:]:
            self.freeMemRDD(nodes_dict[level])

    def makeDPNode(self, geounit_node: GeounitNode, tot_budget=None, dp_queries=True, small_cell_query=False) -> GeounitNode:
        """
        This function takes a GeounitNode with "raw" data and generates
        noisy DP query answers depending the specifications in the
        config object.

        NOTE: This function is called inside the mapper (see above),
        so it is executed for every GeounitNode, on the workers.

        This may be confusing, because the .map() function is called from
        self.noisyAnswers() (above), which is run on the Master node.

        Inputs:
            geounit_node: a Node object with "raw" data
            tot_budget: in minimal schema runs (there are two) the total budget differs, using only a part
            dp_queries: boolean indicating whether dpqueries are present in measurement set, based on config
            small_cell_query: boolean indicating whether a public-historical-data smallCellQuery is present, based on config
        Outputs:
            dp_geounit_node: a Node object with selected DP measurements

        """
        logging.info(json.dumps({'geocode': geounit_node.geocode, 'geolevel': geounit_node.geolevel}))

        # For minimal schema, a part of budget is used in phase 1 and
        # the rest in phase 2, so this function is called with those
        # as tot_budget
        if tot_budget is None:
            tot_budget = self.total_budget

        # For minimal schema phase 1, no DP preset queries are made, so the argument should be set to False
        # Also, no budget is to be spent on the other queries than the detailed one in phase 1
        if dp_queries:
            dp_queries = self.dp_queries
            detailed_prop: float = self.detailed_prop
        else:
            detailed_prop = 1.0

        # For bottom-up (for example), no proportioning over geolevels
        if self.geolevel_prop_budgets is None:
            geolevel_prop = 1.
        else:
            # index relative to the top level
            index = self.levels_reversed.index(geounit_node.geolevel)
            geolevel_prop = self.geolevel_prop_budgets[index]

        node_hist: np.ndarray = geounit_node.getDenseRaw()
        dp_budget: float = tot_budget * geolevel_prop

        dp_geounit_node = geounit_node
        dp_geounit_node.dp_queries = self.nodeDPQueries(dp_budget, node_hist) if dp_queries else {}
        dp_geounit_node.dp = self.makeDPQuery(hist=node_hist, query=querybase.QueryFactory.makeTabularGroupQuery(array_dims=node_hist.shape),
                                              epsilon=detailed_prop * dp_budget)
        if small_cell_query:
            smallCellName = C.SMALLCELLBASENAME + f"_geocode{geounit_node.geocode}"
            multiindices = das_utils.loadJSONFile(self.small_cell_basepath + f"geocode{geounit_node.geocode}.json")
            dp_geounit_node.smallCellQuery = querybase.QueryFactory.makeInefficientCountQuery(array_dims=node_hist.shape,
                                                                                              multiindices=multiindices,
                                                                                              name=smallCellName)
            print(f"In geocode {geounit_node.geocode}, smallCellQuery has answer: {dp_geounit_node.smallCellQuery.answer(node_hist)}")
            assert True == False, "Thou shallt not pass"

        return dp_geounit_node

    def nodeDPQueries(self, dp_budget: float, node_hist: np.ndarray) -> Dict[str, cons_dpq.DPquery]:
        """
        For engines which split the budget between the detailed query and some preset queries,
        make measurements on those preset queries on a given node histogram.
        :param dp_budget: budget dedicated to the preset queries
        :param node_hist: the node histogram
        :return:
        """
        dp_queries = {}
        if self.dp_queries:
            for name, budgetprop in zip(self.dp_query_names, self.dp_query_prop):
                query = self.queries_dict[name]
                dp_query = self.makeDPQuery(hist=node_hist, query=query, epsilon=budgetprop * dp_budget)
                dp_queries.update({name: dp_query})
        return dp_queries

    def makeDPQuery(self, hist, query: querybase.AbstractLinearQuery, epsilon: float) -> cons_dpq.DPquery:
        """
        For the data :hist: and :query: on it, creates DP mechanism to protect it, applies it on the true
        answer, and creates DPquery that contains the query, the dp_answer and mechanism parameters
        :rtype: cons_dpq.DPquery
        """
        dp_mech = self.mechanism(true_data=hist, query=query, epsilon=epsilon, bounded_dp=True)
        return cons_dpq.DPquery(query, dp_mech)

    def checkBoundedDPPrivacyImpact(self, nodes_dict: __NodeDict__) -> (float, float):
        """
           Checks the privacy impact of the queries. For each node in a level, it gets the matrix representations m_i of each query i,
           and also the sensitivity s_i and epsilon e_i used. It computes sum_i eps_i/s_i * (1^T * abs(q_i)) * 2, which represents how
           much privacy budget is used in each cell of the resulting histogram. It then takes the max and min of the cell. The max and
           min are aggregated across the nodes to find the overall max and min of any cell in the level. The max is the privacy impact
           of that level and the gap beween max and min shows wasted privacy budget. The maxes and mins are then summed over levels. The
           function returns two quantities, the first float is the max privacy budget used (the epsilon of the algorithm) and the second
           float is the min.
        """
        self.log_and_print("Checking privacy impact")

        # setup pyspark rdd.aggregate functions
        def seq_op(x, y):
            return max(x[0], y[0]), min(x[1], y[1])
        comb_op = seq_op

        high = 0.0
        low = 0.0
        initial = (-np.inf, np.inf)
        for level, nodes in nodes_dict.items():
            level_hilo = nodes.map(lambda node: node.getBoundedDPPrivacyImpact())  # rdd of (max, min)
            (tmp_hi, tmp_lo) = level_hilo.aggregate(initial, seq_op, comb_op)
            high += tmp_hi
            low += tmp_lo
            self.log_and_print(f"Privacy impact of level {level}: (hi={tmp_hi}, low={tmp_lo})")
        self.log_and_print(f"Privacy impact: {high}\n inefficiency gap: {high - low}\n Set budget: {self.total_budget}")
        # TODO: Fine, if budget is exceeded by a tolerance, e.g. 1e-7. If this what we want?
        assert high <= self.total_budget + 2 ** (-C.BUDGET_DEC_DIGIT_TOLERANCE * 10. / 3.), "Privacy budget accidentally exceeded"

    def checkTotalBudget(self, nodes_dict: __NodeDict__) -> float:
        """
            Sum all epsilons over nodes in each level, divide by number of nodes on that level, then add the level epsilons together
            Or, better, take 1 node from each level and see its budget
        """
        # Dictionary of epsilons for each level
        by_level_eps = {}
        for level, nodes in nodes_dict.items():
            # From each node, get its epsilon
            level_eps = nodes.map(lambda node: node.getEpsilon())
            # Epsilon of some node
            node_eps = level_eps.take(1)[0]

            # If all epsilons are equal, as the should be, this should be sum of near zeros
            print("Checking budgets spent by each node on each level...")
            zero = level_eps.map(lambda neps, eps=node_eps: abs(neps - eps)).reduce(add)
            n_nodes = nodes.count()
            print(f"Geolevel {level}: Sum of epsilon differences (should be 0s) over {n_nodes} nodes: {zero}, relative: {zero/n_nodes}")

            # Epsilon spent on this level
            by_level_eps[level] = node_eps

        used_budget = sum(list(by_level_eps.values()))
        self.log_and_print(f"Used budget: {by_level_eps},\n Total used budget: {used_budget},\n Set budget: {self.total_budget}")
        return used_budget

    def logInvCons(self, nodes: __NodeRDD__) -> None:
        """ Log acting invariants and constraints """
        rec: GeounitNode = nodes.take(1)[0]
        self.log_and_print(f"{rec.geolevel} invariants: " + ", ".join(map(str, rec.invar.keys())))
        self.log_and_print(f"{rec.geolevel} constraints: " + ", ".join(map(str, rec.cons.keys())))


def assertSumTo(values: Iterable, sumto=1., dec_place=C.BUDGET_DEC_DIGIT_TOLERANCE, msg="The ") -> None:
    """
    Assert that sum of the values in the iterable is equal to the desired value with set tolerance
    :param values: iterable, sum of which is to be checked
    :param sumto: float, what it should sum to, default=1.
    :param dec_place: int, tolerance of the sum check, defined by decimal place (approximately, calculated by powers of 2)
    :param msg: Custom error message prefix
    :return:
    """
    errorMsg = f"{msg} values {values} sum to {sum(values)} instead of {sumto}"
    assert(abs(sum(values) - sumto)) < 2 ** (-dec_place * 10. / 3.), errorMsg # f-string won't evaluate properly if is in assert
