"""
manipulate_nodes.py:  The core functionality for manipulating nodes as part of the top-down algorithm.

"""
# python imports
import logging
import numpy as np
from collections import defaultdict
from typing import Tuple, List, Iterable, Union, Callable

# das-created imports
import das_utils
import das_framework.ctools.clogging as clogging

import programs.sparse as sparse
import programs.queries.constraints_dpqueries as cons_dpq
import programs.utilities.numpy_utils as np_utils
import programs.optimization.sequential_optimizers as sequential_optimizers

from programs.nodes.nodes import GeounitNode

import constants as C


def asDense(a: Union[np.ndarray, sparse.multiSparse]) -> np.ndarray:
    if isinstance(a, np.ndarray):
        return a
    return a.toDense()


def geoimp_wrapper(*,config, parent_child_node, accum, min_schema=None):
    """
    This function performs the Post-Processing Step for a generic parent to the Child geography.
    It is called from topdown_engine.py:topdown in a Spark map operation. 
    It runs on the CORE and TASK nodes, not on the MASTER.
    So there is no das object!
    
    Inputs:
        config: configuration object
        parent_child_node: a (k,v) RDD with key being a geocode and
            value being a tuple of GeounitNode objects containing one parent and multiple children
        accum: spark accumulator object which tracks the number of solves that use the backup solve

    Output:
        children: a list of Node objects for each of the children, after post-processing
    """

    # Make sure that the logger is set up on all the nodes
    clogging.setup(level=logging.INFO, syslog='True', 
                   syslog_address=(das_utils.getMasterIp(),C.SYSLOG_UDP))
    parent: GeounitNode
    children: List[GeounitNode]
    parent, children = findParentChildNodes(parent_child_node)

    n_children = len(children)

    #######
    # under cenrtain circumstances we can skip the gurobi optimization
    #######
    #
    # Only 1 child

    if n_children == 1:
        children[0].syn = parent.syn
        return children

    if parent.syn.sum() == 0:
        for child in children:
            child.syn = sparse.multiSparse(np.zeros(parent.syn.shape))
        return children

    #########
    # resume code for gurobi optimization
    ########
    # stack the dp arrays on top of one another, if only 1 child just expand the axis

    if parent.dp:
        if n_children > 1:
            noisy_child = np.stack([asDense(child.dp.DPanswer) for child in children], axis=-1)
        else:
            noisy_child = np.expand_dims(asDense(children[0].dp.DPanswer), axis=len(children[0].dp.DPanswer.shape))
    else:
        noisy_child = None

    noisy_child_weight = 1. / children[0].dp.Var if parent.dp else None

    # TODO: Maybe filtering out the detailed querty form node.dp_queries can be done neater
    dp_queries_comb  = stackNodeProperties(children, lambda node: node.dp_queries, cons_dpq.StackedDPquery, lambda name: name != C.DETAILED)
    query_weights    = map(lambda sdpq: 1./sdpq.Var, dp_queries_comb)  # We can get actual variance for each query if we want
    constraints_comb = stackNodeProperties(children, lambda node: node.cons, cons_dpq.StackedConstraint)
    parent_hist      = parent.getDenseSyn()
    parent_geocode   = parent.geocode

    seq_opt = sequential_optimizers.L2PlusRounderWithBackup(das=None,config=config,
                                                            parent=parent_hist, parent_shape=parent_hist.shape, NoisyChild=noisy_child, 
                                                            childGeoLen=n_children, DPqueries=dp_queries_comb, constraints=constraints_comb,
                                                            NoisyChild_weight=noisy_child_weight, query_weights=query_weights,
                                                            identifier=parent_geocode, min_schema=min_schema, stat_node=children[0])

    l2_answer, int_answer, backup_solve_status = seq_opt.run()

    # slice off the combined child solution to make separate arrays for each child
    int_answer_list = np_utils.sliceArray(int_answer)
    l2_answer_list = np_utils.sliceArray(l2_answer)

    # check constraints
    for i, child in enumerate(children):
        child.syn = int_answer_list[i]
        constraintsCheck(child)

    # make sparse arrays
    for i, child in enumerate(children):
        child.syn = sparse.multiSparse(int_answer_list[i])
        child.syn_unrounded = sparse.multiSparse(l2_answer_list[i])

    if backup_solve_status is True:
        accum += 1

    return children


def geoimp_wrapper_nat(*,config, parent_shape, nat_node: GeounitNode, min_schema=None):
    """
    This function performs the Post-Processing Step of National to National level.
    It is called from engine_utils.py:topdown in a Spark map operation

    Inputs:
        config: configuration object
        nat_node: a GeounitNode object referring to entire nation

    Output:
        nat_node: a GeounitNode object referring to entire nation
    """

    # Make sure that the logger is set up on all of the nodes
    clogging.setup(level=logging.INFO, syslog=True, 
                   syslog_address=(das_utils.getMasterIp(),C.SYSLOG_UDP))
    # t_start = time.time()
    parent_hist = None

    noisy_child = np.expand_dims(asDense(nat_node.dp.DPanswer), axis=len(nat_node.dp.DPanswer.shape)) if nat_node.dp else None
    noisy_child_weight = 1. / nat_node.dp.Var if nat_node.dp else None
    parent_geocode = "nat_to_nat"

    # TODO: Maybe filtering out the detailed querty form node.dp_queries can be done neater
    dp_queries_comb = stackNodeProperties([nat_node, ], lambda node: node.dp_queries, cons_dpq.StackedDPquery, lambda name: name != C.DETAILED)
    query_weights = map(lambda sdpq: 1./sdpq.Var, dp_queries_comb)  # We can get actual variance for each query if we want
    constraints_comb = stackNodeProperties([nat_node, ], lambda node: node.cons, cons_dpq.StackedConstraint)

    # Create an L2PlusRounderWithBackup object
    seq_opt = sequential_optimizers.L2PlusRounderWithBackup(das=None, parent=parent_hist, parent_shape=parent_shape, NoisyChild=noisy_child, childGeoLen=1,
                                                            config=config, DPqueries=dp_queries_comb, constraints=constraints_comb,
                                                            NoisyChild_weight=noisy_child_weight, query_weights=query_weights, identifier="nat_to_nat",
                                                            min_schema=min_schema, stat_node=nat_node)

    l2_answer, int_answer, backup_solve_status = seq_opt.run()

    # get rid of extra dimension
    int_answer = int_answer.squeeze()
    l2_answer  = l2_answer.squeeze()

    nat_node.syn = int_answer
    constraintsCheck(nat_node, parent_geocode)

    nat_node.syn           = sparse.multiSparse(int_answer)
    nat_node.syn_unrounded = sparse.multiSparse(l2_answer)
    return nat_node


def stackNodeProperties(children: Iterable[GeounitNode], get_prop_func: Callable, stacking_func: Callable, key_filter_func: Callable = lambda x: True):
    """
        This function takes a child node, extracts their individual properties and builds
        a stacked version across children for each unique property (constraint or dp_query)
        Inputs:
            children: list of child nodes
            get_prop_func: function that pulls out the property which will be stacked out of a node (e.g. lambda node: node.cons)
            stacking_func: function that creates the stacked object (StackedConstraint or StackedDPquery)
        Outpus:
            stacked_prop_comb: a list of stacked objects
    """
    # children may have different properties/dp_queries. only combine the ones that match.
    # TODO: Do we want to support nodes of the same geo level with different sets of properties/dp_queries? If not, all this can be much simpler.
    #   If we do, what would be a use case?

    # Dictionary: for each constraint/dp_query name as key has list of tuples: (constraint/dp_query, index_of_child_that_has_it)
    stacked_prop_dict = defaultdict(list)
    for i, properties in enumerate(map(get_prop_func, children)):
        # # TODO: Not clear why the following condition and return None is here.
        # #  If a child has empty properties/dp_queries, it just won't appear in any of the indices. Why not return StackedConstraints (StackedDPqueries)
        # #  list with indices of all the other children?
        # if properties is None or not any(properties):
        #     return []  # None
        for key, prop in properties.items():
            if key_filter_func(key):  # This is to filter out the detailed query and not stack it
                stacked_prop_dict[key].append((prop, i))

    stacked_prop_comb = [stacking_func(prop_ind) for key, prop_ind in stacked_prop_dict.items()]

    return stacked_prop_comb


def constraintsCheck(node: GeounitNode, parent_geocode=None):
    """
    This function checks that a set of constraints is met given a solution.
    It will raise an exception if any constraint is not met.
    Inputs:
        node: geounit node with "syn" field to be checked against the node constraints
    """
    if parent_geocode is None:
        parent_geocode = node.parentGeocode
    msg = f"Constraints are for parent geocode {parent_geocode}"
    if not node.checkConstraints(raw=False):
        raise RuntimeError(msg + " failed!")
    print(msg + " satisfied.")


def findParentChildNodes(parent_child_node: Tuple[str, Union[Iterable[GeounitNode], Tuple[Iterable[GeounitNode]]]]) -> Tuple[GeounitNode, List[GeounitNode]]:
    """
    This function inputs an RDD element containing both a parent and child(ren) nodes,
    figures out which is which, and separates them
    Inputs:
        parent_child_node: an (k,v) rdd, value is a tuple containing both a parent and child(ren) nodes, key is parent geocode
    Outputs:
        parent: parent node
        children: list of children nodes
    """

    # Key of (k,v) pair given as argument
    parent_geocode: str = parent_child_node[0]
    # print("parent geocode is", parent_geocode)

    # Value of (k,v) converted to a list of the node objects.
    if isinstance(parent_child_node[1], tuple):
        # (v would be a tuple of (pyspark.resultiterable.ResultIterable, ) if this function is called from spark rdd map() and rdd was .cogroup-ed)
        list_of_nodelists = [list(node) for node in parent_child_node[1]]
        nodes_list: List[GeounitNode] = list_of_nodelists[0] + list_of_nodelists[1]
    else:
        # (v would be pyspark.resultiterable.ResultIterable if this function is called from spark rdd map() and rdd was .groupByKey-ed)
        nodes_list: List[GeounitNode] = list(parent_child_node[1])

    # calculate the length of each of the geocodes (to determine which is the parent)
    geocode_lens = [len(node.geocode) for node in nodes_list]
    # the parent is the shortest geocode
    parent_ind: int = np.argmin(geocode_lens)

    # Alternatively, parent is where geocode is equal to k of (k,v) pair given as argument, also works
    # parent_ind = [node.geocode for node in nodes_list].index(parent_geocode)

    # Get the parent (it's also removed from list)
    parent = nodes_list.pop(parent_ind)

    # Check the the code found by argmin is the same as the parent geocode taken from key of the pair
    assert parent.geocode == parent_geocode

    # sort the children
    children = sorted(nodes_list, key=lambda geocode_data: geocode_data.geocode)

    return parent, children
