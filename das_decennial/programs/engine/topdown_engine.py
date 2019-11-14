"""
Generic Topdown engine with manual workload setup (in form of queries)
topdown_engine.py

specified in a config file as:
[engine]
engine: programs.engine.topdown_engine.TopdownEngine

the run method kicks off the show:
1 - Prints the history gram
2 - makes nodes for all of the geography levels, puts them all in nodes_dict
3 - runs the topdown() method to actually compute the answer.
    (of course, the answers aren't actually computed there; just the RDDs are wired up.
    the answers are actually computed when Spark is told to write the results.)
    topdown() appears in engine_utils.DASEngineHierarchical
"""

# python imports
from typing import Tuple, Dict
from pyspark.sql import SparkSession
import pyspark.accumulators

# das-created imports
import programs.nodes.manipulate_nodes as manipulate_nodes
from programs.engine.engine_utils import DASEngineHierarchical, __NodeRDD__, __NodeDict__
import programs.dashboard as dashboard
import constants as C

__FeasDict__ = Dict[str, pyspark.accumulators.Accumulator]


class TopdownEngine(DASEngineHierarchical):
    """
    Implements engines that employ by-level noise infusion, followed by topdown optimization.
    Examples are: this engine itself, and subclasses HDMMEngine and MinimalSchemaEngine
    """
    def initializeAndCheckParameters(self):
        """
        Superclass, then
        Set per geolevel budgets and per-query budgets
        """
        super().initializeAndCheckParameters()

        # Set budgets on each geolevel
        self.setPerGeolevelBudgets()

        # Set workload /budgets of each query
        self.setWorkload()

    def setWorkload(self):
        """Set budgets of each query. In the most generic Topdown the workload is set up manually"""
        self.setPerQueryBudgets()

    def run(self, original_data: __NodeRDD__) -> Tuple[__NodeRDD__, __FeasDict__]:
        """
        This is the main function of the topdown engine.
        The function makeOrLoadNoisy creates or loads noisy measurements
        The function topdown implements the topdown algorithm.

        Called from das_framework/driver.py

        Inputs:
                self: refers to the object of the engine class
                block_nodes: RDDs of nodes objects for each of the lowest level geography

        Output:
                nodes_dict: a dictionary containing RDDs of node objects for each of the geolevels
                feas_dict: a dictionary containing meta information about optimization
        """

        super().run(original_data)

        self.optimizeWorkload()

        nodes_dict: __NodeDict__ = self.makeOrLoadNoisy(original_data)

        # TODO: These privacy checks should be invoked automatically, either in super().run() or in super().didRun()
        if self.getboolean(C.CHECK_BUDGET, default=True):
            self.checkBoundedDPPrivacyImpact(nodes_dict)
            if abs(self.checkTotalBudget(nodes_dict) - self.total_budget) > 1e-7:
                self.log_warning_and_print("USED BUDGET UNEQUAL TO SET BUDGET!!!")
        else:
            self.log_and_print("Skipping privacy checks")
        # should delete raw here
        nodes_dict, feas_dict = self.topdown(nodes_dict)
        block_nodes = nodes_dict[self.levels_reversed[-1]]

        # Unpersist and delete everything but bottom level
        self.freeMemLevelDict(nodes_dict, down2level=1)

        return block_nodes, feas_dict

    def optimizeWorkload(self):
        """ Nothing to optimize in basic topdown"""
        pass

    def noisyAnswers(self, nodes: __NodeDict__, total_budget: float = None, dp_queries: bool = True, small_cell_query: bool = False) -> __NodeDict__:
        """
        This function is the second part of the topdown engine.
        This function takes the GeounitNode objects at each geolevel, adds differential
        privacy measurements

        Inputs:
            nodes:  a dictionary containing RDDs of node objects for each of the geolevels.
                        No DP measurement or privatized data are present on input

        Output:
            nodes: a dictionary containing RDDs of node objects for each of the geolevels
        """
        nodes_dict: __NodeDict__ = nodes

        if total_budget is None:
            total_budget = self.total_budget

        if dp_queries:
            dp_queries = self.dp_queries

        if small_cell_query:
            small_cell_query = self.small_cell_query

        # for each geolevel take noisy measurements
        for level in self.levels:
            self.annotate(f"Taking noisy measurements at {level}")
            nodes_dict[level] = nodes_dict[level].map(lambda node: self.makeDPNode(node, total_budget, dp_queries, small_cell_query)).persist()
            self.logInvCons(nodes_dict[level])

        return nodes_dict

    def topdown(self, nodes_dict: __NodeDict__, hist_shape: Tuple[int, ...]=None) -> Tuple[__NodeDict__, __FeasDict__]:
        """
        This function is the third part of the topdown engine.
        This function and initiates the topdown algorithm.

        Inputs:
            nodes_dict:  a dictionary containing RDDs of node objects for each of the geolevels.
                         No DP measurement or privatized data are present on input
            hist_shape: for minimal schema, the hist shape is different, so has to be indicated

        Output:
            nodes_dict: a dictionary containing RDDs of node objects for each of the geolevels
            feas_dict: a dictionary containing meta information about optimization
        """
        self.annotate("topdown starting")
        spark = SparkSession.builder.getOrCreate() if self.use_spark else None
        config = self.config

        if hist_shape is None:  # It's only provided for minimal_schema_engine
            hist_shape = self.hist_shape
        if self.minimal_schema:
            min_schema_dims = tuple([self.vars_schema[x] for x in self.minimal_schema])
        else:
            min_schema_dims = None

        # Pool measurements with those from lower level, for each level
        nodes_dict = self.poolLowerLevelMeasurements(nodes_dict)

        # Do Topdown Imputation.


        # This does imputation from national to national level.
        toplevel = self.levels[-1]

        nodes_dict[toplevel] = (
            nodes_dict[toplevel]
            .map(lambda node: manipulate_nodes.geoimp_wrapper_nat(config=config, parent_shape=hist_shape, nat_node=node, min_schema=min_schema_dims))
            .persist()
            )

        ### feasibility dictionary
        feas_dict: __FeasDict__ = {}

        for level, lower_level in zip(self.levels_reversed[:-1], self.levels_reversed[1:]):
            # make accumulator
            feas_dict[level] = spark.sparkContext.accumulator(0) if self.use_spark else 0

            nodes_dict[lower_level] = (
                self.combineParentAndChildren(nodes_dict[level], nodes_dict[lower_level])
                .map(lambda pc_nodes, lev=level: manipulate_nodes.geoimp_wrapper(
                    config=config, parent_child_node=pc_nodes, accum=feas_dict[lev], min_schema=min_schema_dims))
                .flatMap(lambda children: tuple([child for child in children]))
                .persist())

        ### Save the statistics from all levels as a single RDD
        ### Note that collecting statistics is largely a side-effect operation,
        ### so we need to force all of the nodes
        ### to calculate first. count() is an eager action.
        #for level, node_rdd in nodes_dict.items():
        #    self.log_and_print(f"Level {level} RDD has {node_rdd.count()} rows")
        for level in self.levels_reversed:
            self.annotate(f"Geolevel {level} RDD has {nodes_dict[level].count()} rows")

        ### Save the Gurobi statistics if requested to do so
        if self.save_gurobi_stats:
            self.writeGurobiStatistics(spark.sparkContext.union(tuple(nodes_dict.values())))

        # Commented: we do it in run() function. Minimal schema needs the other levels after the first run.
        ### unpersist all but bottom level
        # for level in self.levels_reversed[:-1]:
        #     nodes_dict[level].unpersist()
        #     del nodes_dict[level]
        #     gc.collect()

        ### return dictionary of nodes RDDs
        self.annotate("topdown done")
        return nodes_dict, feas_dict

    def poolLowerLevelMeasurements(self, nodes_dict: __NodeDict__) -> __NodeDict__:
        """
        For each level, combine  measurements with those from lower level
        :return:
        """

        if self.getboolean(C.POOL_MEASUREMENTS, default=False):
            for level, upper_level in zip(self.levels[:-1], self.levels[1:]):
                self.log_and_print(f"Pooling measurements from {level} with {upper_level}...")
                from_lower_rdd = (nodes_dict[level]
                                  .map(lambda node: (node.parentGeocode, node))
                                  .reduceByKey(lambda x, y: x.addInReduce(y, add_dpqueries=True))
                                 )


                nodes_dict[upper_level] = (nodes_dict[upper_level]
                                           .map(lambda node: (node.geocode, node))
                                           .cogroup(from_lower_rdd)
                                           .map(manipulate_nodes.findParentChildNodes)
                                           .map(lambda d: (lambda from_this, from_lower:
                                               from_this.mixMeasurements(from_lower[0]))(*d))
                                          )

                # This is a more transparent but a tiny bit slower way to join
                # .join(from_lower_rdd)
                # .map(lambda d: (lambda geocode, nodes: nodes[0].mixMeasurements(nodes[1]))(*d))

        return nodes_dict

    @staticmethod
    def combineParentAndChildren(level_rdd, lower_level_rdd):
        """
        Given RDD of upper geolevel nodes and rdd of lower geolevel nodes, returns RDD of tuples,
        where each tuple contains a parent and all its child nodes
        :param level_rdd: RDD with all upper level nodes (say, a state)
        :param lower_level_rdd: RDD with all lower level nodes (then, counties)
        :return: RDD with tuples of nodes, parent-and-children within a tuple
        """
        parent_rdd = level_rdd.map(lambda node: (node.geocode, node))
        child_rdd = lower_level_rdd.map(lambda node: (node.parentGeocode, node))

        # Timing of this operation has been measured. It is not negligible, but is dwarfed by Gurobi optimization in
        # the next step, as you go to bigger problems (a single big state is enough to see that). Usually time for
        # union operation is roughly the same as for groupByKey operation

        # parent_child_rdd = parent_rdd.union(child_rdd).groupByKey()
        # parent_child_rdd = parent_rdd.join(child_rdd)

        # Cogroup seems to be slightly faster than union().groupByKey().
        parent_child_rdd = parent_rdd.cogroup(child_rdd)

        return parent_child_rdd

    def deleteTrueData(self, nodes: __NodeDict__) -> __NodeDict__:
        nodes_dict = nodes
        for level in self.levels:
            nodes_dict[level] = super().deleteTrueData(nodes_dict[level])
        return nodes_dict

    def freeMemNodes(self, nodes: __NodeDict__) -> None:
        self.freeMemLevelDict(nodes)
