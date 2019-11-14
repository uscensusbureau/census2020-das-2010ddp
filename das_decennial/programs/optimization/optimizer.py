"""
This is the root class for managing the gurobi optimizer.
"""

# python imports
import logging
import os
import time
import uuid
import sys
import socket
import tempfile
import fnmatch
import subprocess
import socket
from configparser import NoOptionError
from abc import ABCMeta, abstractmethod

## NOTE: Do not import gurobipy at top level, because the path needs to be set first.

import numpy as np

# das-created imports
import programs.queries.querybase as querybase
import programs.utilities.numpy_utils as np_utils
from programs.nodes.nodes import GeounitNode
from programs.queries.constraints_dpqueries import StackedConstraint
import programs.optimization.maps as maps

from das_framework.ctools import clogging as clogging
from das_framework.driver import AbstractDASModule
import das_framework.ctools.s3 as s3
import das_framework.driver as driver

import das_utils
import constants as C
from constants import CC

def ASSERT_TYPE(var, aType):
    if not isinstance(var, aType):
        raise RuntimeError("var is type {} but should be type {}".format(type(var),aType))

def getIP():
    """
    Get the IP address of the current node.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    ip = s.getsockname()[0]
    s.close()
    return ip


class AbstractOptimizer(AbstractDASModule, metaclass=ABCMeta):
    """Specifies common functions for SequentialOptimizer and Optimizer classes
    Inputs:
        config: a configuration object
        stat_node: the node where statistics for running the optimizer should be kept.
    Instance Variables:
        Instance variables that begin 't' are for the statistics system.
        Most of them are times in the optimizer cycle. Comments below
        Because the statistics can be called from the SequentialOptimizer or from an Optimizer,
        they need to be defined here.
    """

    stat_node: GeounitNode   # the node where statistics for running the optimizer should be kept.
    grb_env: object          # Gurobi environment (note: type is gb.Env, but gb cannot be loaded)
    t_uuid: str              # unique identifier for optimizer solve
    t_env_start: float       # the time Gurobi environment creation begins
    t_env_end: float         # the time Gurobi environment creation ends
    t_modbuild_start: float  # when Gurobi begins building the model
    t_modbuild_end: float  # when Gurobi finishes building the model
    t_presolve_start: float  # the time when presolve starts
    t_presolve_end: float    # the time when presolve ends
    t_optimize_start: float  # the time when Optimizer starts
    t_optimize_end: float    # the time when Optimizer ends
    python_presolve: bool
    print_gurobi_stats: bool
    record_gurobi_stats: bool
    record_VM_stats: bool
    record_CPU_stats: bool
    failsafe_invoked: bool

    # pylint: disable=bad-whitespace
    def __init__(self, *, stat_node, **kwargs):
        super().__init__(name=C.GUROBI_SECTION, **kwargs)
        driver.config_apply_environment(self.config) # make sure that environment section is in the environment
        self.stat_node           = stat_node
        self.grb_env             = None   # no grb_env created yet...
        self.t_uuid              = uuid.uuid4().hex  # unique identifier for optimizer solve
        self.t_env_start         = None  # the time Gurobi environment creation begins
        self.t_env_end           = None  # the time Gurobi environment creation ends
        self.t_modbuild_start    = None  # when Gurobi starts building model
        self.t_modbuild_end      = None  # when Gurobi finishes building model
        self.t_presolve_start    = None  # the time when presolve starts
        self.t_presolve_end      = None  # the time when presolve ends
        self.t_optimize_start    = None  # the time when Optimizer starts
        self.t_optimize_end      = None  # the time when Optimizer ends
        self.python_presolve     = self.getboolean(C.PYTHON_PRESOLVE, default=False)
        self.print_gurobi_stats  = self.getboolean(C.PRINT_GUROBI_STATS_OPTION, default=False)
        self.record_gurobi_stats = self.getboolean(C.RECORD_GUROBI_STATS_OPTION, default=False)
        self.record_VM_stats     = self.getboolean(C.RECORD_VM_STATS_OPTION, default=False)
        self.record_CPU_stats    = self.getboolean(C.RECORD_CPU_STATS_OPTION, default=False)
        self.failsafe_invoked    = False
        self.save_lp_path        = self.getconfig(C.SAVE_LP_PATH, default=False, expandvars=True)
        self.save_lp_pattern     = self.getconfig(C.SAVE_LP_PATTERN, default='', expandvars=False)
        os.environ[CC.PYTHON_VERSION] = f'python{sys.version_info.major}.{sys.version_info.minor}'
        self.gurobi_path         = self.getconfig(C.GUROBI_PATH, section=C.GUROBI_SECTION, default=None, expandvars=True)

        if "LD_LIBRARY_PATH" not in os.environ:
            os.environ['LD_LIBRARY_PATH'] = ''
        os.environ['LD_LIBRARY_PATH'] = '/mnt/apps5/gurobi810/linux64/lib' + ":" + os.environ['LD_LIBRARY_PATH']

        if self.gurobi_path and self.gurobi_path not in sys.path:
            sys.path.insert(0, self.gurobi_path)
            try:
                import gurobipy as gb
            except ImportError:
                raise RuntimeError("Cannot import gurobipy; host:{} path:{}".format(socket.gethostname(),"\n".join(sys.path)))

    @abstractmethod
    def run(self):
        """
        To be filled in by subclass.
        This method will specify the sequence of optimizers (in sequential optimizers) and
        build the model, perform optimization(s) and return the result
        """
        pass

    def recordClearedGurobiEnvironment(self):
        """Called when Gurobi environment is cleared to reset the timer."""
        if self.record_gurobi_stats:
            self.t_env_end   = time.time()       # new start time
            self.t_uuid      = uuid.uuid4().hex  # new identifier
            self.stat_node.addGurobiStatistic(optimizer=self, point=CC.POINT_ENV_CREATE)

    def getGurobiEnvironment(self, retries=C.GUROBI_LICENSE_MAX_RETRIES):
        """ Create a new license environment
            IMPORTANT: HAS TO BE NEW ENVIRONMENT, DO NOT TRY TO RETURN ONE ALREADY IN PYTHON OBJECT
        Input:
            config: config file.

        Output:
            environment object

        Notes:
            1. if config["ENVIRONMENT"] is "GAM" or if ISV_NAME is not sent, create an environment using the public
               gb.Env() API, which typically uses the academic license.
            2. If a license cannot be obtained, implements retries with random backoff.

        """

        # This appears to be the first function called in the python environment on each worker node.
        # Be sure the enviornment is propertly set up.

        if self.gurobi_path and self.gurobi_path not in sys.path:
            sys.path.insert(0, self.gurobi_path)
            import gurobipy as gb

        # Syslog does not require the datetime because it
        # is included automatically by protocol, but syslog does not include the year, so we manually add it.
        #
        # NOTE: yarn may not be running on the CORE and TASK nodes
        # when the bootstrap is run, so attempts to set the MASTER_IP
        # on the core nodes sometimes failed. We avoid this now by passing the MASTER_IP
        # in the configuration environment

        clogging.setup(level=logging.INFO, 
                       syslog=True, 
                       syslog_address=(das_utils.getMasterIp(),C.SYSLOG_UDP), 
                       syslog_format=clogging.YEAR + " " + clogging.SYSLOG_FORMAT)

        # THIS BELOW SHOULD NOT BE DONE, THERE'S A REASON FOR RE-CREATING THE ENVIRONMENT
        # # If we already have a grb_env, just return it.
        # if self.grb_env is not None:
        #     return self.grb_env

        os.environ[C.GRB_LICENSE_FILE] = self.getconfig(C.GUROBI_LIC)

        import gurobipy as gb

        # Get environment variables
        cluster = self.getconfig(C.CLUSTER_OPTION, section=C.ENVIRONMENT, default=C.CENSUS_CLUSTER)
        logfile = self.getconfig(C.GUROBI_LOGFILE_NAME)
        isv_name = self.getconfig(C.GRB_ISV_NAME, section=C.ENVIRONMENT, default='')
        app_name = self.getconfig(C.GRB_APP_NAME, section=C.ENVIRONMENT, default='')

        # env = None
        for attempt in range(1, retries):
            try:
                if (cluster == C.GAM_CLUSTER) or (isv_name == ''):
                    # Use academic license
                    env = gb.Env(logfile)
                else:
                    # Use commercial license
                    env3 = self.getint(C.GRB_ENV3, section=C.ENVIRONMENT)
                    env4 = self.getconfig(C.GRB_ENV4, section=C.ENVIRONMENT).strip()
                    env = gb.Env.OtherEnv(logfile, isv_name, app_name, env3, env4)
                    logging.info("Acquired gurobi license on attempt %s", attempt)
                # We got the environment, so break and return it
                return env
            except gb.GurobiError as err:
                # If the environment is not obtained, wait some random time and try again if attempt number is still within range
                rand_wait = (C.GUROBI_LICENSE_RETRY_EXPONENTIAL_BASE ** (attempt - 1) 
                             + np.random.uniform(0, C.GUROBI_LICENSE_RETRY_JITTER))
                logging.info("Failed to acquire gurobi license on attempt %s; waiting %s", (attempt, rand_wait))
                logging.info("(Gurobi error %s)", str(err))
                time.sleep(rand_wait)

        # Attempt number loop is over, ran out of attempts, raise the latest Gurobi error
        raise RuntimeError("Could not acquire Gurobi license, see logfile for more info")


class Optimizer(AbstractOptimizer, metaclass=ABCMeta):
    """
    This is a generic class for gurobi optimization. It includes methods common to generic optimization.
    Superclass Inputs:
        config: a configuration object
        stat_node: the node where statistics for running the optimizer should be kep
    Inputs:
        grb_env: a gurobi environment object. This is a required parameter.
    """
    def __init__(self, *, grb_env, **kwargs):
        super().__init__(**kwargs)
        self.grb_env = grb_env

    # pylint: disable=bad-whitespace
    def newModel(self, model_name):
        """ Creates a new gurobi model utilizing arguments in the self.config object
        Inputs:
            model_name: a string giving the model name
        Output:
            a model object
        """
        import gurobipy as gb
        model = gb.Model(model_name, env=self.grb_env)
        model.Params.LogFile            = self.getconfig(C.GUROBI_LOGFILE_NAME, section=C.GUROBI, default=model.Params.LogFile)
        model.Params.OutputFlag         = self.getint(C.OUTPUT_FLAG,            section=C.GUROBI, default=model.Params.OutputFlag)
        model.Params.OptimalityTol      = self.getfloat(C.OPTIMALITY_TOL,       section=C.GUROBI, default=model.Params.OptimalityTol)
        model.Params.BarConvTol         = self.getfloat(C.BAR_CONV_TOL,         section=C.GUROBI, default=model.Params.BarConvTol)
        model.Params.BarQCPConvTol      = self.getfloat(C.BAR_QCP_CONV_TOL,     section=C.GUROBI, default=model.Params.BarQCPConvTol)
        model.Params.BarIterLimit       = self.getint(C.BAR_ITER_LIMIT,         section=C.GUROBI, default=model.Params.BarIterLimit)
        model.Params.FeasibilityTol     = self.getfloat(C.FEASIBILITY_TOL,      section=C.GUROBI, default=model.Params.FeasibilityTol)
        model.Params.Threads            = self.getint(C.THREADS,                section=C.GUROBI, default=model.Params.Threads)
        model.Params.Presolve           = self.getint(C.PRESOLVE,               section=C.GUROBI, default=model.Params.Presolve)
        model.Params.NumericFocus       = self.getint(C.NUMERIC_FOCUS,          section=C.GUROBI, default=model.Params.NumericFocus)
        try:
            model.Params.timeLimit      = self.getfloat(C.TIME_LIMIT,           section=C.GUROBI, default=model.Params.timeLimit)
        except NoOptionError:
            pass
        return model

    @staticmethod
    def disposeModel(model):
        """
        Gathers the result and model status and deletes the model objects
        Inputs:
            model: gurobi model object
        Outputs:
            mstatus: The Gurobi model status code
        """
        import gurobipy as gb
        mstatus = model.Status
        del model
        gb.disposeDefaultEnv()
        return mstatus


class GeoOptimizer(Optimizer, metaclass=ABCMeta):
    """
    An Optimizer subclass that operates on a parent -> child

    Superclass Inputs:
        config: a configuration object
        grb_env: a gurobi environment object
    Inputs:
        identifier: a string giving identifying information about the optimization (e.g. the parent geocode)
        parent: a numpy multi-array of the parent histogram (or None if no parent)
        parent_shape: the shape of the parent histogram or would be shape if parent histogram is None
        constraints: a list of StackedConstraint objects (see constraints_dpqueries.py)
        childGeoLen: int giving the number of child geographies
        Outputs:
    """

    # pylint: disable=bad-whitespace
    def __init__(self, *, identifier, parent, parent_shape, constraints, childGeoLen, **kwargs):
        super().__init__(**kwargs)
        self.identifier   = identifier
        self.parent       = parent
        self.parent_shape = parent_shape
        self.childGeoLen  = childGeoLen
        self.constraints  = constraints

    def addStackedConstraints(self, model, parent_mask, two_d_vars, rounder=False, child_floor=None):
        """
        Wrapper function that adds constraints to the model
        Inputs:
            model:  gurobi model object
            parent_mask: numpy 1-d boolean array indicating (w/ True) the indexes which then correspond to the children
                         that should be included as variables in the model
            two_d_vars: a two dimensional (variables per geography, number of child geographies) gurobi tuplelist variable object
            rounder: bool indicating if we are using the rounder function (rather than the l2 function)
            child_floor: a multidimensional numpy array used in the rounder
        """
        import gurobipy as gb
        ASSERT_TYPE(model, gb.Model)
        for c in self.constraints:
            self.addConstraint(c, model, parent_mask, two_d_vars, rounder, child_floor)

    @staticmethod
    def addConstraint(stacked_constraint: StackedConstraint, model, parent_mask, two_d_vars, rounder=False, child_floor=None):
        """
        Adds stacked constraints to the model
        Inputs:
            stacked_constraint: StackedConstraint object (see constraints_dpqueries.py)
            model:  gurobi model object
            parent_mask: numpy 1-d boolean array indicating (w/ True) the indexes which then correspond to the children
                         that should be included as variables in the model
            two_d_vars: a two dimensional (variables per geography, number of child geographies) gurobi tuplelist variable object
            rounder: bool indicating if we are using the rounder function (rather than the l2 function)
            child_floor: a multidimensional numpy array used in the rounder
        """
        import gurobipy as gb
        ASSERT_TYPE(model, gb.Model)
        matrix_rep = stacked_constraint.query.matrixRep()[:, parent_mask]
        answer_list = stacked_constraint.rhsList
        if rounder:
            child_floor_list = np_utils.sliceArray(child_floor)
            for counter, level in enumerate(stacked_constraint.indices):
                temp = stacked_constraint.query.answer(child_floor_list[level])
                answer_list[counter] = answer_list[counter] - temp

        sense = maps.toGRBFromStr()[stacked_constraint.sign]
        # Some constraints only appear in a subset of children
        for i in range(len(answer_list[0])):
            xlist = matrix_rep.tocsr()[i, :].indices.tolist()
            for counter, j in enumerate(stacked_constraint.indices):
                expr = gb.LinExpr()
                for x in xlist:
                    expr.add(two_d_vars[x, j])
                model.addConstr(lhs=expr, sense=sense, rhs=answer_list[counter][i], name=stacked_constraint.name)

    def findDesiredIndices(self, backup_feas, min_schema, array=None):
        """
        Finds the proper indices to use for the model depending on the situation
        Inputs:
            array: the numpy array to find non-zero indices (typically self.parent or parent_diff)
            backup_feas: bool indicating if using backup feasibility
            min_schema: list of multidimensional array indexes of the minimal schema
        Outputs:
            parent_mask: 1-d boolean vector over the flattened parent indicating which corresponding cells in the children
                        should be included in the model
            parent_sub: 1-d array of subset (by parent_mask) of parent
        """
        has_parent_flag = array is not None
        parent_mask = np.ones(np.prod(self.parent_shape), dtype=bool)
        if not backup_feas and has_parent_flag:
            parent_mask = array.flatten() > 0
        elif min_schema and has_parent_flag:
            temp = array.sum(min_schema) > 0
            for dim in min_schema:
                temp = np.repeat(np.expand_dims(temp, dim), array.shape[dim], axis=dim)
            parent_mask = temp.flatten()
        parent_sub = array.flatten()[parent_mask] if has_parent_flag else None  #TODO: This returns None if no parent, check
        return parent_mask, parent_sub

    def setObjAndSolve(self, model, obj_fun, child_geo_len=None):
        """
        Sets the objective function for the model and minimizes
        Inputs:
            model: gurobi model object
            obj_fun: gurobi expression of the objective function
        Outputs:
        - Optionally saves the LP file to Amazon S3 if geocode matches save_lp_pattern and save_lp_path is set
        - writes model to /mnt/tmp/grb_hang_{timestr}.lp if GRB.TIME_LIMIT is exceeded.
        """

        import gurobipy as gb
        ASSERT_TYPE(model, gb.Model)
        if self.stat_node.parentGeocode == C.ROOT_GEOCODE:
            model.Params.Threads = self.getint(CC.THREADS_N2N, section=C.GUROBI, default=CC.DEFAULT_THREADS_N2N)
        else:
            # See if there is a geolevel-specific threads override
            threads_geolevel    = CC.THREADS_GEOLEVEL_PREFIX + self.stat_node.geolevel
            model.Params.Threads = self.getint(threads_geolevel, section=C.GUROBI, default=model.Params.Threads)

        model.update()
        model.setObjective(obj_fun, sense=gb.GRB.MINIMIZE)

        if self.save_lp_path:
            if fnmatch.fnmatch( self.stat_node.geocode, self.save_lp_pattern):
                path = os.path.join( self.save_lp_path, self.stat_node.geocode ) + "_" + str(time.time()) + ".lp"
                path=path.replace(' ','_')
                with tempfile.NamedTemporaryFile(suffix=".lp") as tf:
                    model.write(tf.name)
                    cmd = ['/usr/bin/aws','s3','cp',tf.name,path]
                    p = subprocess.Popen(cmd,stderr=subprocess.PIPE)
                    err = p.communicate()[1]
                    if p.returncode!=0 or len(err)>0:
                        raise RuntimeError(f"returncode={p.returncode} err={err} cmd={cmd}")

        if self.record_gurobi_stats:
            self.stat_node.addGurobiStatistic(childGeoLen=child_geo_len, model=model,
                                              optimizer=self, point=CC.POINT_PRESOLVE_START)

        if self.python_presolve:
            # Explicitly call presolve in the python API
            error = ''
            error_message = ''
            try:
                self.t_presolve_start = time.time()
                model.presolve()
                self.t_presolve_end = time.time()

            except gb.GurobiError as e:
                error = 'presolve'
                error_message = str(e)

            if self.record_gurobi_stats:
                self.stat_node.addGurobiStatistic(childGeoLen=child_geo_len,
                                                  model=model, optimizer=self,
                                                  point=CC.POINT_PRESOLVE_END,
                                                  error=error, error_message=error_message)
            self.t_presolve_start = None
            self.t_presolve_end = None
            
        error = ''
        error_message = ''
        try:
            self.t_optimize_start = time.time()
            model.optimize()
            self.t_optimize_end = time.time()

        except gb.GurobiError as e:
            error = 'optimize'
            error_message = str(e)

        if self.record_gurobi_stats:
            self.stat_node.addGurobiStatistic(childGeoLen=child_geo_len, model=model,
                                              optimizer=self,
                                              did_presolve=self.python_presolve,
                                              point=CC.POINT_OPTIMIZE_END,
                                              error=error, error_message=error_message)
            self.t_optimize_start=None
            self.t_optimize_end=None
            
        if model.Status == gb.GRB.TIME_LIMIT:
            timestr = time.strftime("%Y%m%d-%H%M%S")
            model.write(f"/mnt/tmp/grb_hang_{timestr}.lp")

        if self.print_gurobi_stats:
            model.printStats()

    @staticmethod
    def buildAndAddParentConstraints(model, parent_sub, two_d_vars, n, name):
        """
        Adds the parent constraints to the model
        Inputs:
            model: gurobi model object
            parent_sub: 1-d array of subset (by parent_mask) of parent
            two_d_vars: a two dimensional (variables per geography, number of child geographies) gurobi tuplelist variable object
            n: int giving the number of variables in the first dim of two_d_vars
            name: str giving the name of the constraint
        """
        import gurobipy as gb
        ASSERT_TYPE(model, gb.Model)
        for i in range(n):
            temp=model.addConstr(lhs=two_d_vars.sum(i, '*'), sense=gb.GRB.EQUAL, rhs=parent_sub[i], name = name + str(i))

    def buildBackupObjFxn(self, model, two_d_vars, array, array_sub, parent_mask, min_schema, name=None):
        """
        Builds the backup objective function in the l2 and rounder classes and adds constraints
        ensuring that the non-minimal schema marginals are kept equal.
        Inputs:
            model: gurobi model object
            two_d_vars: a two dimensional (variables per geography, number of child geographies) gurobi tuplelist variable object
            array: numpy multidimensional array (either parent or parent_diff)
            array_sub: 1-d array of subset of array by the parent_mask
            parent_mask: 1-d boolean vector over the flattened parent indicating which corresponding cells in the children
                        should be included in the model
            min_schema: list of multidimensional array indexes of the minimal schema
            name: str giving the name of the constraint
        Outputs:
            backup_obj_fun: gurobi expression giving the backup objective function
        """
        import gurobipy as gb
        ASSERT_TYPE(model, gb.Model)
        rhs = array_sub
        backup_obj_fun = gb.LinExpr()
        n = int(np.sum(parent_mask))
        slack = model.addVars(n, vtype=gb.GRB.CONTINUOUS, lb=0.0, name="backup")
        for i in range(n):
            constexpr = gb.LinExpr(-1.0 * two_d_vars.sum(i, '*'))
            constexpr += rhs[i]
            tmp = model.addConstr(lhs=slack[i], sense=gb.GRB.GREATER_EQUAL, rhs=constexpr)
            constexpr2 = gb.LinExpr(two_d_vars.sum(i, '*'))
            constexpr2 += -rhs[i]
            tmp2 = model.addConstr(lhs=slack[i], sense=gb.GRB.GREATER_EQUAL, rhs=constexpr2)
            backup_obj_fun.add(slack[i])

        # Add marginal constraint
        if min_schema:
            parent_mask_ms = array.sum(min_schema).flatten() > 0
            rhs = array.sum(min_schema).flatten()[parent_mask_ms]
            back_marg_query = querybase.QueryFactory.makeTabularGroupQuery(self.parent_shape, add_over_margins=min_schema)
            tmp_kron = back_marg_query.matrixRep()
            row_ind = parent_mask_ms
            matrix_rep = tmp_kron[:, parent_mask][row_ind, :]

            for i in range(rhs.size):
                xlist = matrix_rep.tocsr()[i, :].indices.tolist()
                expr = gb.LinExpr()
                for x in xlist:
                    expr += gb.LinExpr(two_d_vars.sum(x, '*'))
                model.addConstr(lhs=expr, sense=gb.GRB.EQUAL, rhs=rhs[i], name=name+"_"+str(i))

        return backup_obj_fun

    def backupFeasOptimize(self, model, backup_obj_fun):
        """
        Set and optimize the backup objective function, print information and return the objective value
        Inputs:
            model: gurobi model object
            backup_obj_fun: gurobi expression giving the backup objective function
        Outputs:
            model.ObjVal: the objective value of the model after optimizing
        """
        import gurobipy as gb
        ASSERT_TYPE(model, gb.Model)
        self.setObjAndSolve(model, backup_obj_fun, child_geo_len=self.childGeoLen)
        fail_msg = f"On {getIP()}, degree-of-infeasibility {model.ModelName} backup solve failed with status {model.Status}. " \
            f"This should not happen! Check that min schema is specified correctly. Exiting program..."
        assert model.Status == gb.GRB.OPTIMAL, fail_msg
        return model.ObjVal

    @staticmethod
    def backupFeasAddConstraint(model, backup_obj_val, backup_obj_fun, obj_slack=1.0):
        """
        Add the constraint to the model based on the backup objective optimized value plus some slack
        Inputs:
            model: gurobi model object
            backup_obj_val: the objective value of the model after optimizing
            backup_obj_fun: gurobi expression giving the backup objective function
            obj_slack: float >= 0 giving the amount of slack provided to the constraint
        """
        import gurobipy as gb
        ASSERT_TYPE(model, gb.Model)
        model.addConstr(lhs=backup_obj_fun, sense=gb.GRB.LESS_EQUAL, rhs=backup_obj_val+obj_slack)


