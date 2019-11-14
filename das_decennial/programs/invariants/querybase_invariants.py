""" Module to store swappable invariant creator classes """
import logging
from typing import Union, Iterable, Dict, Any, Callable
import numpy as np
import programs.queries.querybase as querybase
import programs.sparse

__HistData__ = Union[programs.sparse.multiSparse, np.ndarray]


class AbstractInvariantsCreator:
    """ New super class for invariant creators """
    invariants_dict: Dict[str, np.ndarray]
    invariant_names: Iterable[str]
    invariant_funcs_dict: Dict[str, Callable[[Any], None]]

    def __init__(self, raw: __HistData__, raw_housing: __HistData__, invariant_names: Iterable[str]):

        if raw is None:
            msg = "Person histogram should be present to create invariants"
            logging.error(msg)
            raise ValueError(msg)
        if isinstance(raw, programs.sparse.multiSparse):
            self.raw = raw.toDense()
        elif isinstance(raw, np.ndarray):
            self.raw = raw
        else:
            raise TypeError(f"Person histogram should be either {programs.sparse.multiSparse} or {np.ndarray}, not {type(raw)}")

        if raw_housing is None:
            msg = "Housing histogram should be present to create invariants"
            logging.error(msg)
            raise ValueError(msg)
        if isinstance(raw_housing, programs.sparse.multiSparse):
            self.raw_housing = raw_housing.toDense()
        elif isinstance(raw_housing, np.ndarray):
            self.raw_housing = raw_housing
        else:
            raise TypeError(f"Housing histogram should be either {programs.sparse.multiSparse} or {np.ndarray}, not {type(raw_housing)}")

        self.invariant_names = invariant_names
        self.invariant_funcs_dict = {}
        self.invariants_dict = {}

    def calculateInvariants(self):
        """
        Actually calculate all invariants indicated in invariant_funcs_dict
        :return:
        """
        for name in self.invariant_names:
            assert name in self.invariant_funcs_dict, f"Provided invariant name '{name}' is not implemented."
            self.invariant_funcs_dict[name]()
        return self

    def addToInvariantsDict(self, data, add_over_margins, name, groupings=None):
        """
        Make invariant from a query and data and add to invariants dict
        """
        query = querybase.QueryFactory.makeTabularGroupQuery(array_dims=data.shape, groupings=groupings,
                                                             add_over_margins=add_over_margins, name=name)
        self.invariants_dict[name] = np.array(query.answerWithShape(data)).astype(int)
