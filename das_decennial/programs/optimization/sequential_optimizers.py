"""This file implements the following classes:

L2PlusRounderWithBackup 
  A sequentialOptimizer that runs the GeoRound
  optimizer and, if it fails, invokes the failsafe mechanism.

  This optimizer is called from manipulate_nodes:geoimp_wrapper()
   
"""

import json
import time
from abc import ABCMeta, abstractmethod

#python imports
import logging
import numpy as np

#das-created imports
from programs.optimization.optimizer import AbstractOptimizer
import programs.optimization.geo_optimizers as geo_optimizers

#constansts file
import constants as C
from constants import CC

class SequentialOptimizer(AbstractOptimizer, metaclass=ABCMeta):
    """
    This specifies a class that runs a single or sequence of optimizations.
    Superclass Inputs:
        config: a configuration object
        stat_node: the node where statistics for running the optimizer should be kep
    Inputs:
        grb_env should NOT be provided, because this one creates its own.
    """
    def __init__(self, **kwargs):
        assert 'grb_env' not in kwargs
        super().__init__(**kwargs)
        self.gurobiEnvironment()

    def gurobiEnvironment(self):
        """
        This method is a wrapper for a function that creates a new gurobi Environment
        This wrapper exists for a reason. DO NOT try to cut it out in __init__ or getGurobiEnvironment
        """
        self.t_env_start = time.time()      # start time for creating environment
        self.grb_env     = self.getGurobiEnvironment()
        self.recordClearedGurobiEnvironment()



class L2PlusRounderWithBackup(SequentialOptimizer):
    """
    L2GeoOpt + geoRound w/ degree-of-infeasibility failsafe
        Inputs:
            identifier: a string giving identifying information about the optimization (e.g. the parent geocode)
            parent: a numpy multi-array of the parent histogram (or None if no parent)
            parent_shape: the shape of the parent histogram or would be shape if parent histogram is None
            childGeoLen: int giving the number of child geographies
            constraints: a list of StackedConstraint objects (see constraints_dpqueries.py)
            NoisyChild: numpy multidimensional array of noisy measurments of the detailed child cells
            NoisyChild_weight: float giving the coefficient for the optimization function for each NoisyChild cell
            DPqueries: a list of StackedDPquery objects (see constraints_dpqueries.py)
            query_weights: a list of floats giving the coefficent for the optimization function for each cell of each query or None
            min_schema: list of minimal schema dimensions
        Superclass Inputs:
            config: a configuration object
    """
    def __init__(self, *, identifier, parent, parent_shape, childGeoLen, constraints, NoisyChild,
                 NoisyChild_weight, DPqueries, query_weights, min_schema, **kwargs):
        super().__init__(**kwargs)
        self.identifier = identifier
        self.parent = parent
        self.parent_shape = parent_shape
        self.childGeoLen = childGeoLen
        self.constraints = constraints
        self.NoisyChild = NoisyChild
        self.NoisyChild_weight = NoisyChild_weight
        self.DPqueries = DPqueries
        self.query_weights = query_weights
        self.min_schema = min_schema

    def run(self):
        """
        Runs on the CORE nodes, not on the master.
        The main method running the chain of optimizations:
        Outputs:
            L2opt.answer: numpy multi-array, the un-rounded solution
            Rounder.answer: numpy multi-array, the rounded solution
            self.failsafe_invoked: bool, indicating if the failsafe was invoked
        """
        # Standard L2
        import gurobipy as gb
        logging.info(json.dumps({'identifier':self.identifier,
                                 'loc':'sequential_optimizers:run'}))
        L2opt = geo_optimizers.L2GeoOpt(das = self.das, config = self.config, grb_env=self.grb_env, identifier=self.identifier,
                                        parent = self.parent, parent_shape=self.parent_shape, 
                                        childGeoLen=self.childGeoLen, 
                                        NoisyChild=self.NoisyChild, NoisyChild_weight=self.NoisyChild_weight,
                                        constraints=self.constraints,DPqueries=self.DPqueries,
                                        query_weights=self.query_weights, min_schema=self.min_schema,nnls=True,
                                        backup_feas=False, stat_node=self.stat_node)
        L2opt.run()
        if L2opt.mstatus == gb.GRB.OPTIMAL:
            # Standard L1
            Rounder=geo_optimizers.GeoRound(das = self.das, config = self.config, grb_env=self.grb_env,identifier=self.identifier,
                                             parent=self.parent,parent_shape=self.parent_shape,
                                             constraints=self.constraints,
                                             childGeoLen=self.childGeoLen, child=L2opt.answer,
                                             min_schema=self.min_schema,backup_feas=False, stat_node=self.stat_node)
            Rounder.run()
            self.checkModelStatus(Rounder.mstatus,"Rounder_standard",C.L1_FEAS_FAIL)
        else:
            # L2 did not find optimal solution; invoke the failsafe
            self.failsafe_invoked        = True
            L2opt.backup_feas            = True
            L2opt.use_parent_constraints = False


            self.stat_node.addGurobiStatistic(optimizer=self, point=CC.POINT_FAILSAFE_START)
            L2opt.run()
            self.checkModelStatus(L2opt.mstatus,"L2_failsafe",C.L2_FEAS_FAIL)

            logging.info(json.dumps({'msg':'Running failsafe for L1',
                                     'identifier':self.identifier,
                                     'min_schema':self.min_schema}))
            # Failsafe L1
            Rounder=geo_optimizers.GeoRound(das= self.das, config=self.config, grb_env=self.grb_env,identifier=self.identifier,
                                            parent=self.parent,constraints=self.constraints,
                                            parent_shape=self.parent_shape,childGeoLen=self.childGeoLen,
                                            child=L2opt.answer,min_schema=self.min_schema, 
                                            backup_feas=True, stat_node=self.stat_node)
            Rounder.run()
            self.checkModelStatus(Rounder.mstatus,"Rounder_failsafe",C.L1_FEAS_FAIL)
            logging.info(json.dumps({'msg':'L1 change in parent b/c of failsafe',
                                     'val': int(np.sum(np.abs(self.parent-Rounder.answer.sum((-1,))))) } ))


            self.stat_node.addGurobiStatistic(optimizer=self, point=CC.POINT_FAILSAFE_END)
        del self.grb_env
        logging.info(json.dumps({'identifier':self.identifier,
                                 'L2opt.mstatus':L2opt.mstatus,
                                 'msg':'L2 model END',
                                 'source':'syslog'}))
        return L2opt.answer, Rounder.answer, self.failsafe_invoked

    def checkModelStatus(self, mStatus, Phase, msg):
        """  
        Checks the status of the model, if not optimial, raises an exception
        Inputs:
            mStatus: gurobi model status code
            Phase: str indicating the (L2 or Rounder) phase
            msg: str, a message to print if not optimal
        """
        import gurobipy as gb
        if mStatus != gb.GRB.OPTIMAL:
            del self.grb_env
            raise Exception(f"{msg} Failure encountered in geocode {self.identifier} after {Phase}. Model status was {mStatus}.")
