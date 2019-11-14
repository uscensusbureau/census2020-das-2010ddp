##
## Note:
##
## Do not import gurobipy at top level. 
## sys.path() is set depending on which version of gurib we are using. 
## path is set in AbstractOptimizer.
##


import numpy as np
import time


# das-created imports
from programs.optimization.optimizer import GeoOptimizer as GeoOptimizer

# constansts file
from constants import CC
import constants as C

def ASSERT_TYPE(var, aType):
    if not isinstance(var, aType):
        raise RuntimeError("var is type {} but should be type {}".format(type(var),aType))

class L2GeoOpt(GeoOptimizer):

    """
    This function solves the L2 estimation problem for geography imputation. Takes DP
    measurements and finds a nearby (non-negative if nnls) possibly fractional solution.
        Inputs:
            NoisyChild: numpy multidimensional array of noisy measurments of the detailed child cells
            NoisyChild_weight: float giving the coefficient for the optimization function for each NoisyChild cell
            DPqueries: a list of StackedDPquery objects (see constraints_dpqueries.py)
            query_weights: a list of floats giving the coefficent for the optimization function for each cell of each query or None
            nnls: bool if True, impose >=0 constraint on estimated counts
            backup_feas: bool if true run in backup feasibility mode
            min_schema: list of minimal schema dimensions
        Superclass Inputs:
            config: a configuration object
            grb_env: a gurobi envirnoment object
            identifier: a string giving identifying information about the optimization (e.g. the parent geocode)
            parent: a numpy multi-array of the parent histogram (or None if no parent)
            parent_shape: the shape of the parent histogram or would be shape if parent histogram is None
            constraints: a list of StackedConstraint objects (see constraints_dpqueries.py)
            childGeoLen: int giving the number of child geographies
    """
    def __init__(self, *, NoisyChild=None, NoisyChild_weight=None, DPqueries=None, 
                query_weights=None, nnls=True, backup_feas=False, min_schema=None, **kwargs):
        super().__init__(**kwargs)
        self.nnls: bool        = nnls
        self.backup_feas: bool = backup_feas
        self.NoisyChild        = NoisyChild
        self.NoisyChild_weight = NoisyChild_weight
        self.DPqueries         = DPqueries
        self.query_weights     = query_weights
        self.min_schema: list  = min_schema
        self.answer            = None
        self.mstatus           = None
        self.use_parent_constraints = not self.backup_feas and self.parent is not None
        
    def run(self):
        """
        The main function.  Builds the models, solves and returns the answer.
        """



        print(f"L2 model for parent {self.identifier}")
        
        # Instantiate model, set GRB params
        self.t_modbuild_start = time.time()
        l2_model = self.newModel("persondata")

        # Prepare data, subset to nonzeros
        # TODO: This returns None for parent_sub if backup_feas and no parent, which prevents creating backup_obj_func, check
        parent_mask, parent_sub  = self.findDesiredIndices(self.backup_feas, self.min_schema, array=self.parent)
        n = np.sum(parent_mask) # The number of desired indices from the parent
        print(f"n is {n}")

        # Build variables
        import gurobipy as gb
        two_d_vars = self.buildMainVars(l2_model, n, gb.GRB.CONTINUOUS)

        # Build objective function
        obj_fun = gb.QuadExpr()
        if self.NoisyChild is not None:
            NoisyChild_sub = self.NoisyChild.reshape(np.prod(self.parent_shape), self.childGeoLen)[parent_mask, :]
            self.addSquaredObjNoisyChild(two_d_vars, NoisyChild_sub, obj_fun, self.NoisyChild_weight)

        if self.DPqueries:
            self.addDPQueriesToModel(l2_model, two_d_vars, obj_fun, parent_mask)

        # Build backup feasibility 2ndary optimization function
        if self.backup_feas:
            backup_obj_fun = self.buildBackupObjFxn(l2_model, two_d_vars, self.parent, parent_sub,
                                                 parent_mask, self.min_schema, name=C.BACKUP)

        # Build parent constraints
        if self.use_parent_constraints:
            self.buildAndAddParentConstraints(l2_model, parent_sub, two_d_vars, n, name=C.PARENT_CONSTRAINTS)

        # Build other constraints
        if self.constraints is not None:
            self.addStackedConstraints(rounder=False, model=l2_model, parent_mask=parent_mask, two_d_vars=two_d_vars)


        # If in backup, optimize to build degree-of-infeasibility constraint
        if self.backup_feas:
            backup_obj_val = self.backupFeasOptimize(l2_model, backup_obj_fun)
            self.backupFeasAddConstraint(l2_model, backup_obj_val, backup_obj_fun)

        # Log model build start/end times
        if self.record_gurobi_stats:
            self.t_modbuild_end = time.time()
            self.stat_node.addGurobiStatistic(optimizer=self, point=CC.POINT_MODBUILD_END)
            self.t_modbuild_end = None
            self.t_modbuild_start = None
            
        # Primary obj fxn optimization
        self.setObjAndSolve(l2_model, obj_fun, child_geo_len=self.childGeoLen)

        self.answer = self.reformSolution(two_d_vars, n, parent_mask) if l2_model.Status == gb.GRB.OPTIMAL else None

        # Cleanup
        self.mstatus = self.disposeModel(l2_model)


    def buildMainVars(self, model, n, vtype=None, name="main_cells"):
        """
        Builds the primary variables for the model
        Inputs:
            model: gurobi model object
            n: int, length of first dim of variables
            vtype: the variable type binary, continuous etc.
            name: str giving the variables a name
        Output:
            two_d_vars: a two dimensional (variables per geography, number of child geographies) gurobi tuplelist variable object 
        returns type -> gb.tupledict, but we can't annotate as such because gb is not loaded when this file is imported.
        """
        import gurobipy as gb
        ASSERT_TYPE(model, gb.Model)
        if vtype is None:
            vtype = gb.GRB.CONTINUOUS
        lb = 0 if self.nnls else -gb.GRB.INFINITY
        two_d_vars: gb.tupledict = model.addVars(int(n), int(self.childGeoLen), vtype=vtype, lb=lb, name=name)
        return two_d_vars
    
    def addDPQueriesToModel(self, model, two_d_vars, obj_fun, parent_mask):
        """
        Adds the DP queries information to the objective function
        Inputs:
            model: gurobi model object
            two_d_vars: a two dimensional (variables per geography, number of child geographies) gurobi tuplelist variable object
            obj_fun: the gurobi expression used as the objective function
            parent_mask: numpy 1-d boolean array indicating (w/ True) the indexes which then correspond to the children
                         that should be included as variables in the model
        """
        ## Time estimates below are from profiling with PL94 topdown_RI, postprocessing only (so percentage of topdown runtime)
        ## Whole function:
        ## (master, nd.array hist): Own time 36.6%, total time 63%
        import gurobipy as gb
        ASSERT_TYPE(model, gb.Model)
        ASSERT_TYPE(two_d_vars, gb.tupledict)
        the_weights = [1.0 for _ in self.DPqueries] if self.query_weights is None else self.query_weights  # Other query obj fxn terms
        lb = 0 if self.nnls else -gb.GRB.INFINITY
        if self.DPqueries:

            for (st_dpq, weight) in zip(self.DPqueries, the_weights):
                # 0% own time, ~0.7% total time
                # (master) 0, 0.9
                query = st_dpq.query
                matrix_rep = query.matrixRep()[:, parent_mask]
                num_aux_vars_per_geo = matrix_rep.shape[0]
                reduced_childGeoLen = len(st_dpq.indices)
                # ~4% time
                # (master) 5.5%
                aux_vars = model.addVars(int(num_aux_vars_per_geo), int(reduced_childGeoLen), vtype=gb.GRB.CONTINUOUS, lb=lb, name=st_dpq.name)

                for i in range(num_aux_vars_per_geo):
                    # ~0.3% own time, ~11% total time (seems to be mostly csr.__getitem__)
                    #--------
                    # ~0%, 0.1% total
                    mrep_csr = matrix_rep.tocsr()  # Isn't it already CSR from querybase?
                    # (sparse) ~0.1% own time, ~5.8% total
                    # (master) 0.1, 7.6
                    mrep_csr_slice = mrep_csr[i, :]
                    # mrep_csr_slice = matrix_rep.tocsc()[i, :].tocsr()
                    # ~0
                    indices = mrep_csr_slice.indices
                    multiplier = mrep_csr_slice.data

                    for counter, j in enumerate(st_dpq.indices):
                        # Add aux vars constraints
                        # ~9.5% own time, ~12% total
                        # ----
                        # (master) 1.6%, 3.6%
                        cons_expr = gb.LinExpr()
                        for index, mult in zip(indices, multiplier):
                            # ~0.3% own time, ~0.4% total (both in master and sparse)
                            cons_expr.add(two_d_vars[index, j], mult)
                        # (sparsehist) ~8.3% own time, ~9.8% total
                        # (master/dense) 10.3, 12.2
                        model.addConstr(lhs=cons_expr, sense=gb.GRB.EQUAL, rhs=aux_vars[i, counter], name="m_cons" + st_dpq.name)

                        # Add terms to obj fxn
                        # ~13.1% own time, ~22.6% total
                        #---------
                        # ~0.4% own time, ~0.4% total (both master and sparse)
                        dp_answer_multi = st_dpq.DPanswerList[counter]
                        # ~0.5% own time, ~2.3% total
                        # (master) 0.6, 2.8
                        DPanswer = dp_answer_multi.flatten()
                        # ~9% own time, ~14% total
                        # (master 11%, 17%)
                        expr = gb.LinExpr(aux_vars[i, counter] - DPanswer[i])   # alternatively, np.unravel_index(i, dp_answer_multi.shape)

                        # # Alternatively, lose the aux_vars altogether and just add the main_vars directly to the obj_fun and no constraints for aux.
                        # # This seems to move the job from here to optimization (as tested on PL94 RI) and be a bit slower
                        # expr = gb.LinExpr(multiplier, [two_d_vars[index, j] for index in indices])
                        # expr.addConstant(-DPanswer[i])

                        # ~4.4% own time, ~7.7% total
                        # (master) 5.4%, 9.7%
                        obj_fun.add(expr=expr * expr, mult=weight)

    @staticmethod
    def addSquaredObjNoisyChild(two_d_vars, noisy_child_sub, obj_fun, obj_fun_weight):
        """
        Adds squared error to the objective function for the noisychild
        Inputs:
            two_d_vars: a two dimensional (variables per geography, number of child geographies) gurobi tuplelist variable object
            noisy_child_sub: two-d numpy array, subset of noisy child according to the parent_mask
            obj_fun: gurobi expression giving the objective function
            obj_fun_weight: float, coefficent for each cell of the objective function
        """
        import gurobipy as gb
        ASSERT_TYPE(two_d_vars, gb.tupledict)

        m, n = noisy_child_sub.shape
        for i in range(m):
            for j in range(n):
                expr = gb.LinExpr(two_d_vars[i,j] - noisy_child_sub[i,j])
                obj_fun.add(expr*expr, mult=obj_fun_weight)

    def reformSolution(self, two_d_vars, n, parent_mask):
        """
        Translate the variables solution back to the NoisyChild shape
        Inputs:
            two_d_vars: a two dimensional (variables per geography, number of child geographies) gurobi tuplelist variable object
            n: int, length of first dimension of two_d_vars
            parent_mask: numpy 1-d boolean array indicating (w/ True) the indexes which then correspond to the children
                         that should be included as variables in the model
        Outputs:
            result: Numpy Multiarray, The optimized solution back in the shape of NoisyChild
        """
        import gurobipy as gb
        ASSERT_TYPE(two_d_vars, gb.tupledict)
        sol = np.array( [[two_d_vars[i,j].X  for j in range(self.childGeoLen)] for i in range(n) ])
        temp_result = np.zeros((np.prod(self.parent_shape), self.childGeoLen))
        temp_result[parent_mask,:] = sol
        result = np.reshape(temp_result,self.parent_shape+(self.childGeoLen,))
        if self.nnls:
            result[result < 1e-7] = 0
        return result


class GeoRound(GeoOptimizer):
    """
        This function solves the integer rounding problem for geography imputation. Takes a
        non-negative possibly fractional solution and finds a nearby non-negative integer solution.
        Inputs:
            child: a numpy array that is a non-negative solution to the L2 problem
            backup_feas: bool, if true run in backup feasibility mode
            min_schema: list giving dimensions of minimal schema or None
    """
    def __init__(self, *, child, backup_feas, min_schema=None, **kwargs):
        super().__init__(**kwargs)

        self.child = child
        self.backup_feas:bool = backup_feas
        self.min_schema:tuple = min_schema

        self.answer  = None
        self.mstatus = None
        self.use_parent_constraints = not self.backup_feas and self.parent is not None

    def run(self):
        """
        Main method, builds the model, optimizes and stores the answer
        """

        print(f"Rounder model for parent {self.identifier}")
        import gurobipy as gb

        # Instantiate model, set GRB params
        self.t_modbuild_start = time.time()
        rounder_model = self.newModel("int_persondata")

        # Prepare data, subset to nonzeros
        child_floor, child_leftover, parent_diff = self.childFloor()
        parent_mask, parent_diff_sub = self.findDesiredIndices(self.backup_feas, self.min_schema, array=parent_diff)
        n = np.sum(parent_mask) # The number of desired indices from the parent
        child_leftover_sub = child_leftover.reshape((np.prod(self.parent_shape), self.childGeoLen))[parent_mask,:]

        # Build variables
        two_d_vars = self.buildMainVars(rounder_model, n)

        # Build objective function
        obj_fun = gb.LinExpr()
        self.buildObjFun(obj_fun, two_d_vars, child_leftover_sub)

        # Build backup feasibility 2ndary optimization function
        if self.backup_feas:
            backup_obj_fun = self.buildBackupObjFxn(rounder_model, two_d_vars, parent_diff, parent_diff_sub,
                                                     parent_mask, self.min_schema, name=C.ROUND_BACKUP)
        # Build parent constraints
        if not self.backup_feas and self.parent is not None:
            self.buildAndAddParentConstraints(rounder_model, parent_diff_sub, two_d_vars, n, name=C.PARENT_CONSTRAINTS_ROUND)
        # Build other constraints
        if self.constraints is not None:
            self.addStackedConstraints(rounder_model, parent_mask, two_d_vars, rounder=True, child_floor=child_floor)

        # If in backup, optimize to build degree-of-infeasibility constraint
        if self.backup_feas:
            backup_obj_val = self.backupFeasOptimize(rounder_model, backup_obj_fun)
            self.backupFeasAddConstraint(rounder_model, backup_obj_val, backup_obj_fun)
    
        # Log model build start/end times
        if self.record_gurobi_stats:
            self.t_modbuild_end = time.time()
            self.stat_node.addGurobiStatistic(optimizer=self, point=CC.POINT_MODBUILD_END)
            self.t_modbuild_end = None
            self.t_modbuild_start = None
                
        # Standard optimization (maybe + degree-of-infeasibility constraint)
        self.setObjAndSolve(rounder_model, obj_fun, child_geo_len=self.childGeoLen)
    
        # Extract solution
        self.answer = self.reformSolution(two_d_vars, n, child_floor, parent_mask) if rounder_model.Status == gb.GRB.OPTIMAL else None
    
        # Cleanup
        self.mstatus = self.disposeModel(rounder_model)

    def buildMainVars(self, model, n, name="main_cells" ):
        """
        Builds the main variables for the models
        Inputs:
            model: gurobi model objects
            n: int, length of first dimension of variables
            name: str, give the variables a name
        """
        import gurobipy as gb
        ASSERT_TYPE(model, gb.Model)

        two_d_vars = model.addVars(int(n), int(self.childGeoLen), vtype=gb.GRB.BINARY, lb=0.0, ub = 1.0)
        return two_d_vars

    def buildObjFun(self, obj_fun, two_d_vars, child_leftover_sub):
        """
        Adds the main objective function for the rounder
        Inputs:
            obj_fun: gurobi expression for the objective function
            two_d_vars: a two dimensional (variables per geography, number of child geographies) gurobi tuplelist variable object
            child_leftover_sub: two-d numpy multi array, child-child_floor subset to mask
        """
        import gurobipy as gb
        ASSERT_TYPE(two_d_vars, gb.tupledict)

        n = child_leftover_sub.shape[0]
        for i in range(n):
            for j in range(self.childGeoLen):
                expr = gb.LinExpr(two_d_vars[i,j])
                obj_fun.add(expr, mult=1-2*child_leftover_sub[i,j])
    
    def childFloor(self):
        """
        Calculate the child_floor, child_leftover, and parent_diff
        Outputs:
            child_floor: numpy multi array, floor(child)
            child_leftover: numpy multi array, child-child_floor
            parent_diff: numpy multi array, parent - sum of child_floor across geographies
        """
        child_floor = np.floor(self.child)
        child_leftover = self.child - child_floor
        geo_dim = len(self.child.shape)-1
        parent_diff = self.parent - np.sum(child_floor, geo_dim ) if self.parent is not None else None
        return child_floor, child_leftover, parent_diff

    def reformSolution(self, two_d_vars, n, child_floor, parent_mask):
        """
        Take the solution from the model and turn it back into the correctly shaped numpy multi array
        Inputs:
            two_d_vars: a two dimensional (variables per geography, number of child geographies) gurobi tuplelist variable object
            n: int, length of first dimension of variables
            child_floor: numpy multi array, floor(child)
            parent_mask: numpy 1-d boolean array indicating (w/ True) the indexes which then correspond to the children
                         that should be included as variables in the model
        Outputs: 
            result: Numpy Multiarray, The optimized solution back in the shape of NoisyChild
        """
        sol = np.array( [[two_d_vars[i,j].X  for j in range(self.childGeoLen)] for i in range(n) ])
        result = child_floor.reshape((np.prod(self.parent_shape), self.childGeoLen))
        if n > 0:
            result[parent_mask,:] = result[parent_mask,:] + sol
        result = result.reshape(child_floor.shape)
        result = np.around(result)
        result = result.astype(int)
        return result
