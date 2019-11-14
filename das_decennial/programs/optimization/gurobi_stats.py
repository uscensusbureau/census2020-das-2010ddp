# Gurobi performance statistics that we capture:
# http://www.gurobi.com/documentation/7.5/refman/attributes.html#sec:Attributes
GUROBI_MODEL_ATTRS= "NumVars,NumConstrs,NumSOS,NumQConstrs,NumGenConstrs,NumNZs,NumQNZs,NumIntVars,NumBinVars,NumPWLObjVars,MIPGap,Runtime,Status,IterCount,BarIterCount,NodeCount,IsMIP,IsQP,IsQCP".split(",")

# http://www.gurobi.com/documentation/7.5/refman/parameters.html#sec:Parameters
GUROBI_MODEL_PARAMS="Threads,TuneCriterion".split(",")


def model_info(model):
    """Return a dictionary of the model attributes and parameters that we care about"""
    ret = {}
    for name in GUROBI_MODEL_ATTRS:
        try:
            ret[name] = model.getAttr(name)
        except AttributeError:
            pass

    for name in GUROBI_MODEL_PARAMS:
        try:
            ret[name] = getattr(model.Params,name)
        except AttributeError:
            pass

    ret['model_status'] = model.status

    import gurobipy as gb
    ret['gurobi_version'] = ".".join((map(str,gb.gurobi.version())))
    return ret
