
## This module is problematic, because it should not import gurobipy when read.
##


def toGRBFromStr():
    import gurobipy as gb
    """ Module for program-wide constant maps (e.g., dicts that should never change) """
    return {"=":gb.GRB.EQUAL,
     "le": gb.GRB.LESS_EQUAL,
     "ge": gb.GRB.GREATER_EQUAL}
