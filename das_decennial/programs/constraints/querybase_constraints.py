"""
Implements a generic Constraints Creator and a ConstraintCreator for schema with HHGQ
"""
from typing import Tuple, Dict, Iterable, Callable, Union
import logging
import warnings
import numpy as np

from programs.constraints.constraint_dict import ConstraintDict
import programs.queries.constraints_dpqueries as cons_dpq
import programs.schema.schema as sk

from constants import HHGQ


__InvariantsDict__ = Dict[str, np.ndarray]


class AbstractConstraintsCreator:
    """ New super class for constraint creators
        This class is what is used to create constraint objects from what is listed in the config file
    Inputs:
        hist_shape: the shape of the underlying histogram
        invariants: the list of invariants (already created)
        constraint_names: the names of the constraints to be created (from the list below)
    """
    invariants: __InvariantsDict__
    constraint_names: Iterable[str]
    hist_shape: Tuple[int, ...]
    constraints_dict: ConstraintDict
    constraint_funcs_dict: Dict[str, Callable]
    schema: sk

    def __init__(self, hist_shape: Tuple[int, ...], invariants: __InvariantsDict__, constraint_names: Iterable[str]):
        self.invariants = invariants
        self.constraint_names = constraint_names
        self.hist_shape = hist_shape
        # self.constraints_dict = {}
        self.constraints_dict = ConstraintDict()
        self.constraint_funcs_dict = {}

    def calculateConstraints(self):
        """ Perform actual constraint calculations for every constraint in the constraint_funcs_dict"""
        for name in self.constraint_names:
            assert name in self.constraint_names, "Constraint name '{}' not implemented.".format(name)
            self.constraint_funcs_dict[name](name=name)
        return self

    def addToConstraintsDict(self, name: str, query_name: Union[str, Iterable], rhs: np.ndarray, sign: str) -> None:
        """
        Make constraint from query name, sign and right hand side and add to the dict
        """
        # query = querybase.QueryFactory.makeTabularGroupQuery(array_dims=array_dims, groupings=groupings, add_over_margins=add_over_margins,
        # name=name)
        query = self.schema.getQuery(query_name)
        if name in self.constraints_dict:
            msg = f"Constraint {name} already exists, and is being replaced!!!"
            logging.warning(msg)
            warnings.warn(msg)
        self.constraints_dict[name] = cons_dpq.Constraint(query=query, rhs=rhs, sign=sign, name=name)

    def setSchema(self, buildfunc):
        """ Set which schema is used"""
        self.schema = buildfunc()
        assert self.hist_shape == self.schema.shape, f"Histogram shape in data {self.hist_shape} doesn't correspond to histogram shape in chosen " \
            f"schema {self.schema.shape}"

    def checkInvariantPresence(self, name, inv_names):
        """ Check whether all the invariants used in a constraint are present"""
        for mand_inv in inv_names:
            if mand_inv not in self.invariants:
                err_msg = f"To calculate {name} constraints, the invariant '{mand_inv}' should be present as well"
                logging.error(err_msg)
                raise ValueError(err_msg)


class ConstraintsCreatorwHHGQ(AbstractConstraintsCreator):
    """ Include common calculations for 1940, PL94, PL94_P12, DHCP, SF1"""
    gqt_dim: int        # Dimensions of GQ-type vector = number of GQ-types + 1 (for housing units)
    hhgq_cap: np.array  # upper bound on number of of people in a gq or hh
    hhgq_lb: np.array   # lower bound on number of of people in a gq or hh

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
        self.hhgq_lb = np.repeat(1, self.gqt_dim)  # 1 person per building (TODO: we don't have vacant in the histogram, it's going to change)

    def calculateRightHandSideHhgqTotal(self, upper):
        """
        Calculate lower and upper bounds for total number of people in each GQ-type
        If total number of people invariant:
            Upper:
                Number of people in each gqhh type is <= total -(#other gq facilities),
                or has no one if there are no facilities of type
            Lower:
                 Number of ppl in each gqhh type is >= number of facilities of that type,
                or is everyone if it is the only facility type
        If total number of people can change, then upper bound is maximum occupancy of each type multiplied by number of units of that type,
        and lower is 1 person per each unit
        :param upper: True if calculating the upper bound, False if calculating the lower bound
        """
        self.checkInvariantPresence("hhgq_total_ub/lb", ('gqhh_tot', 'gqhh_vect'))

        num_units_of_type = self.invariants["gqhh_vect"]
        total_units = self.invariants["gqhh_tot"]

        # How many minimum people we have to put in each gq type, which is set by self.hhgq_lb vector,
        # so we multiply number of units by that vector
        # (it's [1,1,1...] for "1-person-per-unit" and [0,1,1,1,1...] when we have vacant housing units)
        lb_per_type = num_units_of_type * self.hhgq_lb

        # For each hhgq value hhgq_i, number of units with other hhgq values,
        # i.e. total number of units minus units with hhqg=hhgq_i  # (gq_hh in other cats)
        # This is used to know how many people we can put in in units of other types, when calculating lower bound
        other_gqs = total_units - num_units_of_type

        # how many people have to be in units of other types (when calculating upper bound),
        # To find it, sum all the minimum numbers of people in each type, and subtract the value for current type,
        # i.e. mpiog_i = \sum(lpt) - lpt_i
        min_people_in_other_gqs = sum(lb_per_type) - lb_per_type

        if "tot" in self.invariants.keys():
            # Total number of people, broadcasted to units histogram dims, (gqt_dim,)
            total_people = np.broadcast_to(np.array([self.invariants["tot"]]), (self.gqt_dim,))
            if upper:
                # Upper bound:
                # Per docstring: If number of units is non-zero, then total_people - 1 (or self.hhgq_lb) person per each unit in other hhgq values, otherwise 0
                rhs1 = np.where(num_units_of_type > 0, total_people - min_people_in_other_gqs, np.zeros(self.gqt_dim))

                # Number of units times maximum occupancy
                rhs2 = num_units_of_type * self.hhgq_cap

                # Take cell-wise minimum of the two
                return np.minimum(rhs1, rhs2).astype(int)

            # Lower bound:
            # If there are units with other hhgq values,
            #   take the number of units if this type (lower bound is 1 person per unit (or self.hhgq_lb), and everyone else goes to other types),
            #   otherwise take total number of people (no other types, so everyone has to be in this type)
            # (Like the docstring says)
            return np.where(other_gqs > 0, lb_per_type, total_people).astype(int)

        # Upper bound: number of units of this type times maximum occupancy
        # Lower bound: number of units of this type (1 person per unit is the minimum)
        return (num_units_of_type * (self.hhgq_cap if upper else self.hhgq_lb)).astype(int)

    def calculateRightHandSideHhgqVa(self, upper):
        """
        Common calculation of the right hand sides for HHGQ x VA queries (at the moment used in PL94, PL94_12, 1940
        :param upper: True for upper bounds, False for lower bounds
        :return: array of dimensions (self.gqt, 2) where self.gqt_dim is the dimension of the GQ-types vector.
        Typically, number of GQ-types + 1 (for non-GQ)
        So there is one row per GQ type and 2 columns, for voting and non-voting
        TODO: This assumes self.hhgq_lb is [1,1,1,1...] i.e. 1 person per unit, no vacant units
        """
        self.checkInvariantPresence("hhgq_va_ub/lb", ('tot', 'va', 'gqhh_vect'))

        # Number of units with each hhgq (minimum number of people per GQ TYPE)
        gq_min = self.invariants["gqhh_vect"]

        # indices with >0 facilities of type
        cats = np.where(gq_min > 0)[0]
        # distinct gqhh types w/ >0 facilities of type
        num_cats = sum(gq_min > 0)

        # Broadcast to 2D with 2 identical columns (for nva/minors and va/adults respectively) and number of rows equal to number of GQ types
        gq_min_ext = np.broadcast_to(gq_min, (2, self.gqt_dim)).transpose()

        total_val = self.invariants["tot"]  # "tot" invariant is a scalar, total number of people
        va_val = self.invariants["va"][0]  # "va" invariant is an array with single element, so taking that element

        # Array of 1 row for TOTAL number of people: [non_voting, voting]
        nva_va = np.array([total_val - va_val, va_val])

        # Proliferate to multiple rows, one row per GQ type.
        # (Note, it's still TOTAL number of people, not the people in that GQ type, it's identical rows)
        nva_va_ext = np.broadcast_to(nva_va, (self.gqt_dim, 2))

        # The same array with va and nva columns flipped
        va_nva_ext = nva_va_ext[:, ::-1]

        # Array of zeros of the shape of the rhs: two columns for voting/non-voting and self.gqt_dim rows for each GQ type
        zeros = np.zeros((self.gqt_dim, 2))

        # if there is just 1 gqhh facility type with >0 facilities, then other facility types have ub(=lb) of 0 persons
        # and the distinguished facility type has ub(=lb) of nva_va (total minors, total adults)
        if num_cats == 1:
            # Everything is 0,
            rhs = zeros
            # except for that only hhgq category (taking a single row (from gqt_dim identical rows) in nva_va)
            rhs[cats, :] = nva_va
        elif upper:
            # Calculate the upper bounds for each GQ Type, when there are many GQ types

            # Lower bounds on people in units of other other GQtype for each GQtype
            # (=number of units of other GQtype since 1 person per unit is the min)
            other_gqs_lb = np.array([np.sum(gq_min) - gq_min]).transpose()  # Total number of units - number of units of this type
            other_gqs_lb_ext = np.broadcast_to(other_gqs_lb, (self.gqt_dim, 2))  # Expand dims to have a column for non-voting and column for voting

            # (If facilities of this type exist and there are adults, then the description below, otherwise upper bound is 0    )
            # Upper bound for adults is the total number of adults minus all the adults we have to put in other facilities,
            # so that they have a minimum of 1 person. If there is enough minors to fill the other facilities, then the
            # upper bound for adults in this facility is total number of adults. Otherwise, we have to put some of the adults
            # in the other facilities, namely N_adults2move = N_facilities - N_minors.
            # To calculate this:
            # 1) "Minimum number of people in other GQ types" minus "total minors" = "adults to put to other GQ types" =
            #    = "adults to subtract from total to get upper bound"
            # 2) If positive, the upper bound is "total adults" - "adults to subtract from total to get upper bound"
            # 3) If not positive, then there is enough minors to fill the other GQ types, so the upper bound is equal to
            #    total number of adults (we subtract zero)
            #
            # Swap adults <-> minors in description above to get the calculation for upper bound on minors
            # Calculations below do adults and minors simultaneously, in the first and second column respectively

            # other_gqs_lb_ext - va_nva_ext # Number of [minors, adults] to put to other facilities
            rhs = np.where(np.multiply(gq_min_ext > 0, nva_va_ext > 0).astype(bool),  # if facility count & nva_va > 0, ub on gqhh x nva va cell is
                           # nva_va - max(other_gqs_lb - va_nva, 0)
                           nva_va_ext - np.where(other_gqs_lb_ext - va_nva_ext > 0, other_gqs_lb_ext - va_nva_ext, zeros),
                           # if either facility count or nva_va is 0, ub on gqhh x nva_va cell is 0
                           zeros)
        else:
            # Calculate the lower bounds for each GQ Type, when there are many GQ types

            # The should be minimum of gq_min people in this facility type. Preferably, of the other va_group.
            # That is, if we are calculating the lower bound on adults, we want gq_min minors.
            # If there are enough (>=gq_min) minors, then the lower bound on adults is zero.
            # If there are not enough, then some adults must remain in this facility too, namely,
            # N_{adults-to-remain} = N_{units-of-this-type}-N_{total_minors}
            # Hence, if the later is positive, the lower bound is that, if it is negative (i.e. there are enough
            # minors to fill facilities of this type, then the lower bound on adults is 0).
            # Swap adults <-> minors for the minors calculation.
            # Calculations for [minors, adults] are performed in the first and second column of the array

            # gq_min_ext - va_nva_ext # Minimum number of [minors, adults] to put into facilities of this type
            rhs = np.squeeze(np.where(gq_min_ext - va_nva_ext > 0, gq_min_ext - va_nva_ext, zeros))

        return rhs.astype(int)
