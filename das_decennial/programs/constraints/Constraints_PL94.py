"""
Constraints for PL94 schema.
ConstraintCreator class is an actual PL94 constraint creator.

ConstraintCreator inherits from ConstraintsCreatorPL94Generic the actual methods creating constraints,
but has its own __init__
"""
import numpy as np
import programs.constraints.querybase_constraints as querybase_constraints
import programs.schema.schemas.Schema_PL94 as Schema_PL94

from constants import HHGQ, VOTING


class ConstraintsCreatorPL94Generic(querybase_constraints.ConstraintsCreatorwHHGQ):
    """
    This class implements creation of constraints for PL94, PL94_12, 1940 (possibly, etc..)
    """

    def nurse_nva_0(self, name):
        """
        Structural zero: no minors in nursing facilities (GQ code 301)
        """
        self.addToConstraintsDict(name, ("nurse", "nonvoting"), np.array(0), "=")

    def total(self, name):
        """
        Total population per geounit must remain invariant
        """
        self.checkInvariantPresence(name, ('tot',))
        self.addToConstraintsDict(name, "total", self.invariants["tot"].astype(int), "=")

    def voting_age(self, name):
        """
        Total voting age population per geounit must remain invariant
        """
        self.checkInvariantPresence(name, ('va',))
        self.addToConstraintsDict(name, "voting", self.invariants["va"].astype(int), "=")

    def hhgq_total_lb(self, name):
        """
        Lower bound on number of people in each GQ type
        Right-hand side calculation is implemented in the parent class in querybase_constraints.py
        """
        rhs = self.calculateRightHandSideHhgqTotal(upper=False)
        self.addToConstraintsDict(name, HHGQ, rhs, "ge")

    def hhgq_total_ub(self, name):
        """
        Upper bound on number of people in each GQ type
        Right-hand side calculation is implemented in the parent class in querybase_constraints.py
        """
        rhs = self.calculateRightHandSideHhgqTotal(upper=True)
        self.addToConstraintsDict(name, HHGQ, rhs, "le")

    def hhgq_va_ub(self, name):
        """
        Upper bound on cross of gqhh & va
        Right-hand side calculation is implemented in the parent class in querybase_constraints.py
        """
        rhs = self.calculateRightHandSideHhgqVa(upper=True)
        self.addToConstraintsDict(name, (HHGQ, VOTING), rhs, "le")

    def hhgq_va_lb(self, name):
        """
        Lower bound on cross of gqhh & va
        Right-hand side calculation is implemented in the parent class in querybase_constraints.py
        """
        rhs = self.calculateRightHandSideHhgqVa(upper=False)
        self.addToConstraintsDict(name, (HHGQ, VOTING), rhs, "ge")


class ConstraintsCreator(ConstraintsCreatorPL94Generic):
    """
        This class is what is used to create constraint objects from what is listed in the config file, for PL94 schema
        see base class for keyword arguments description

        The variables in PL94 are: hhgq voting hispanic cenrace ( schema:  {'hhgq': 0, 'voting': 1, 'hispanic': 2, 'cenrace': 3} )
        With ranges: 0-7, 0-1, 0-1, 0-62 respectively
        Thus the histograms are 4-dimensional, with dimensions (8,2,2,63)
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.constraint_funcs_dict = {
            "total":         self.total,
            "hhgq_total_lb": self.hhgq_total_lb,
            "hhgq_total_ub": self.hhgq_total_ub,
            "voting_age":    self.voting_age,
            "hhgq_va_ub":    self.hhgq_va_ub,
            "hhgq_va_lb":    self.hhgq_va_lb,
            "nurse_nva_0":   self.nurse_nva_0
        }

        self.setSchema(Schema_PL94.buildSchema)
        self.setHhgqDimsAndCaps()
