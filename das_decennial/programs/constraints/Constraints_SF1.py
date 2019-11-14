import numpy as np
import programs.constraints.querybase_constraints as querybase_constraints
import programs.schema.schemas.Schema_SF1 as Schema_SF1

#from constants import *

HHGQ = 'rel'

# Master to-do list for constraints
#[] min age = 0, max age = 115
#[] hh, spouse, parent, parent-in-law, child-in-law, housemate/roommate, unmarried parter can't be 0-14 age
#[] child, adopted child, stepchild can't be >=90
#[] parent/parent-in-law can't be <=29
#[]


class ConstraintsCreator(querybase_constraints.ConstraintsCreatorwHHGQ):
    """
    This class is what is used to create constraint objects from what is listed in the config file
    Inputs:
        hist_shape: the shape of the underlying histogram
        invariants: the list of invariants (already created)
        constraint_names: the names of the constraints to be created (from the list below)
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.constraint_funcs_dict = {
            "total"                     : self.total,
            "hhgq_total_lb"             : self.hhgq_total_lb,
            "hhgq_total_ub"             : self.hhgq_total_ub,
    #        "nurse_nva_0"               : self.nurse_nva_0,
            "no_refhh_under_15"         : self.no_refhh_under_15,
            "no_kids_over_89"           : self.no_kids_over_89,
            "no_parents_under_30"       : self.no_parents_under_30,
            # "no_foster_over_20"          : self.no_foster_over_20 -> not in this data
    #        "no_spouse_range"           : self.no_spouse_range
        }

        # It's tested and works, but for now decided not to couple this to schema module
        self.setSchema(Schema_SF1.buildSchema)
        # Setting explicitly instead
        # self.schema = (HHGQ, SEX, AGE, HISP, CENRACE)
        # self.all_axes = (0, 1, 2, 3, 4)

        # Number of non-gq categories
        self.hh_cats: int = 15

        # Dimensions of GQ-type vector = number of GQ-types + 1 (for non-GQ)
        self.gqt_dim: int = None  # set in self.setHhgqDimsAndCaps()

        # ub cap on number of of people in a gq or hh
        self.hhgq_cap: np.array = None  # set in self.setHhgqDimsAndCaps()

        self.setHhgqDimsAndCaps()

    # def setSchema(self, buildfunc):
    #     self.schema_full = buildfunc()
    #     self.schema = self.schema_full.dimnames
    #     assert self.hist_shape == self.schema_full.shape, f"Histogram shape in data {self.hist_shape} doesn't correspond to histogram shape in chosen " \
    #         f"schema {self.schema_full.shape}"
    #     self.all_axes = tuple(range(0, len(self.schema_full.shape)))

    def setHhgqDimsAndCaps(self):
        """
        Set dimension of hhgq axis and caps
        """
        # 43 HHGQ cats minus 15 HH cats + 1 for all HH(non-GQ)
        self.gqt_dim = self.hist_shape[self.schema.dimnames.index(HHGQ)] - self.hh_cats + 1
        # ub cap on number of of people in a gq or hh
        self.hhgq_cap = np.repeat(99999, self.gqt_dim)
        # households
        self.hhgq_cap[0] = 99
        # low bound
        self.hhgq_lb = np.repeat(1, self.gqt_dim)  # 1 person per building (TODO: we don't have vacant in the histogram, it's going to change)


    #def no_spouse_range(self):
    #    for i in range(15,116):
    #        name = "spouse_range_{}".format(i)
    #        array_dims = self.hist_shape
    #        query1 = blah
    #        query2 = ugh
    #        cons_dpq.Constraint(query=[query1, query2])...


        # spouse_i + partner_i <= sum_j \in [i-50,i+50] hh_j

    # what about looking at ranges? How many ranges are there?
    #65=[15, 115], 66=[16,115], 67=[17,115], 68 = [18,115] 90 = [40,115], 115=[65,115]



    #def no_foster_over_20(self):
    #    name = "no_foster_over_20"
    #    array_dims = self.hist_shape
    #    groupings = {0: [[]]}




    def no_parents_under_30(self, name):
        self.addToConstraintsDict(name, "parents * under30yearsTotal",  np.array(0), "=")


    def no_kids_over_89(self, name):
        self.addToConstraintsDict(name, "children * over89yearsTotal", np.array(0), "=")



    def no_refhh_under_15(self, name):
        """
        The following can't be under 15:
        'Householder (0)', 'Husband/Wife (1)', 'Father/Mother (6)', 'Parent-in-law (8)', 'Son/Daughter-in-law (9)', 'Housemate, Roommate (12)',
        'Unmarried Partner (13)'
        """
        self.addToConstraintsDict(name, "refhhunder15", np.array(0), "=")

    # Structural zero: no minors in nursing facilities (GQ code 301)
    #def nurse_nva_0(self):
    #    name = "nurse_nva_0"
    #    array_dims = self.hist_shape
    #    groupings = {
    #                0: [[3]],
    #                1: [[0]]
    #                }
    #    add_over_margins = (2,3,4)

    #    rhs = np.array(0)
    #    sign = "="
    #    self.addToConstraintsDict(array_dims, add_over_margins, name, groupings, rhs, sign)

    # Total population per geounit must remain invariant
    def total(self, name):
        """
        Total population per geounit must remain invariant
        """
        self.checkInvariantPresence(name, ('tot',))
        # Total means summing over all variables, hence all_axes
        self.addToConstraintsDict(name, "total", self.invariants["tot"].astype(int), "=")

    def hhgq_total_lb(self, name):
        """
        Lower bound on number of people in each GQ type
        Right-hand side calculation is implemented in the parent class in querybase_constraints.py
        """
        rhs = self.calculateRightHandSideHhgqTotal(upper=False)
        self.addToConstraintsDict(name, "gqhh", rhs, "ge")

    def hhgq_total_ub(self, name):
        """
        Upper bound on number of people in each GQ type
        Right-hand side calculation is implemented in the parent class in querybase_constraints.py
        """
        rhs = self.calculateRightHandSideHhgqTotal(upper=True)
        self.addToConstraintsDict(name, "gqhh", rhs, "le")

    def calculateRightHandSideHhgqTotal(self, upper):
        """
        Just calling super class for now.
        Leaving it here to attract attention to this fact.

        The calculation is as follows:
            Upper:
                Number of people in each gqhh type is <= total -(#other gq facilities),
                or has no one if there are no facilities of type
            Lower:
                Number of ppl in each gqhh type is >= number of facilities of that type,
                or is everyone if it is the only facility type

        The only constraint used in this calculation is "1 person per each facility" (i.e. number of occupied housing
        units is invariant). In this case (SF1), for GQ type 0, it will be the householder, but this does not seem to affect any of these
        calculations.
        It has different answers for when total population is kept invariant or not.
        Total number of housing units and numbers of units of each GQ type have to be kept invariant.
        """
        return super().calculateRightHandSideHhgqTotal(upper)
