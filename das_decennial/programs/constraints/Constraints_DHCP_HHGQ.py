"""
Constraints for DHCP_HHGQ schema.
ConstraintCreator class is an actual DHCP_HHGQ constraint creator.

ConstraintCreator inherits from ConstraintsCreatorDHCGeneric the actual methods creating constraints,
but has its own __init__
"""
import numpy as np
import programs.constraints.querybase_constraints as querybase_constraints
import programs.schema.schemas.Schema_DHCP_HHGQ as Schema_DHCP_HHGQ

from constants import HHGQ


class ConstraintsCreatorDHCGeneric(querybase_constraints.ConstraintsCreatorwHHGQ):
    """
    This class implements creation of constraints for PL94, PL94_12, 1940 (possibly, etc..)
    """
        
    def hhgq1_lessthan15(self,name):
        """
        Structural zero: No <15 ages in adult correctional facilities
        """
        self.addToConstraintsDict(name, ("hhgq_1", "age_lessthan15"), np.array(0), "=")

    def hhgq2_greaterthan25(self,name):
        """
        Structural zero: No >25 ages in juvenile facilities
        """
        self.addToConstraintsDict(name, ("hhgq_2", "age_greaterthan25"), np.array(0), "=")

    def hhgq3_lessthan20(self,name):
        """
        Structural zero: No <20 ages in nursing facilities
        """
        self.addToConstraintsDict(name, ("hhgq_3", "age_lessthan20"), np.array(0), "=")

    #No age restrictions for other institutional facilities
    
    def hhgq5_lt16gt65(self,name):
        """
        Structural zero: No <16 or >65 ages in college housing
        """
        self.addToConstraintsDict(name, ("hhgq_5", "age_lt16gt65"), np.array(0), "=")

    def hhgq6_lt17gt65(self,name):
        """
        Structural zero: No <17 or >65 ages in military housing
        """
        self.addToConstraintsDict(name, ("hhgq_6", "age_lt17gt65"), np.array(0), "=")

    #No age restrictions for other non-institutional facilities

    def setHhgqDimsAndCaps(self):
        """
        Set dimension of hhgq axis and caps
        """
        super().setHhgqDimsAndCaps()
        # households include vacant housing units, so lower bound is 0
        self.hhgq_lb[0] = 0

    def nurse_nva_0(self, name):
        """
        Structural zero: no minors in nursing facilities (GQ code 301)
        """
        self.addToConstraintsDict(name, ("gqNursingTotal", "nonvoting"), np.array(0), "=")

    def total(self, name):
        """
        Total population per geounit must remain invariant
        """
        self.checkInvariantPresence(name, ('tot',))
        self.addToConstraintsDict(name, "total", self.invariants["tot"].astype(int), "=")

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

    
class ConstraintsCreator(ConstraintsCreatorDHCGeneric):
    """
        This class is what is used to create constraint objects from what is listed in the config file, for PL94 schema
        see base class for keyword arguments description

        The variables in DHCP_HHGQ are: hhgq voting hispanic cenrace ( schema:  {'hhgq': 0, 'sex': 1, 'age': 2,  'hispanic': 3, 'cenrace': 4, 'citizen': 5} )
        With ranges: 0-7, 0-1, 0-115, 0-1, 0-62, 0-1 respectively
        Thus the histograms are 6-dimensional, with dimensions (8,2,116,2,63,2)
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.constraint_funcs_dict = {
            "total":         self.total,
            "hhgq_total_lb": self.hhgq_total_lb,
            "hhgq_total_ub": self.hhgq_total_ub,
            "nurse_nva_0":   self.nurse_nva_0,
            "hhgq1_lessthan15" : self.hhgq1_lessthan15,
            "hhgq2_greaterthan25" : self.hhgq2_greaterthan25,
            "hhgq3_lessthan20" : self.hhgq3_lessthan20,
            "hhgq5_lt16gt65" : self.hhgq5_lt16gt65,
            "hhgq6_lt17gt65" : self.hhgq6_lt17gt65
        }

        self.setSchema(Schema_DHCP_HHGQ.buildSchema)
        self.setHhgqDimsAndCaps()
