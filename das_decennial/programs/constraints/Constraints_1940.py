"""
Constraints for 1940 schema. Largely inherited from PL94
"""
import programs.constraints.Constraints_DHCP_HHGQ as Constraints_DHCP_HHGQ
import programs.schema.schemas.schemamaker as schemamaker
from constants import CC
import numpy as np

HHGQ = CC.ATTR_HHGQ_1940

class ConstraintsCreator(Constraints_DHCP_HHGQ.ConstraintsCreator):
    """
    This class is what is used to create constraint objects from what is listed in the config file
    Inputs:
        hist_shape: the shape of the underlying histogram
        invarinats: the list of invaraints (already created)
        constraint_names: the names of the constraints to be created (from the list below)
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.constraint_funcs_dict = {
            "total"                     : self.total,
            "hhgq_total_lb"             : self.hhgq_total_lb,
            "hhgq_total_ub"             : self.hhgq_total_ub
        }

        self.setSchema(buildfunc=None)
        self.setHhgqDimsAndCaps()

    def setSchema(self, buildfunc):
        """ Set which schema is used"""
        self.schema = schemamaker.buildSchema(name=CC.DAS_1940)
        assert self.hist_shape == self.schema.shape, f"Histogram shape in data {self.hist_shape} doesn't correspond to histogram shape in chosen " \
            f"schema {self.schema.shape}"

    def setHhgqDimsAndCaps(self):
        """
        Set dimension of hhgq axis and caps
        """
        # Find index of HHGQ variable in the schema and look the dimension in the histogram shape
        self.gqt_dim = self.hist_shape[self.schema.dimnames.index(HHGQ)]
        # ub cap on number of of people in a gq or hh
        self.hhgq_cap = np.repeat(99999, self.gqt_dim)
        # households
        self.hhgq_cap[0] = 99
        # low bound
        self.hhgq_lb = np.repeat(1, self.gqt_dim)

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