"""
Module implements ConstraintDict class, which is a dict that checks its values to be of Constraint type,
and ability to add to ConstraintDicts
"""
import logging
import programs.queries.constraints_dpqueries as cons_dpq

class ConstraintDict(dict):
    """
    Enhances standard dict for the following purposes:
        1) Make sure that the values can only be queries.constraints_dpqueries.Constraint
        2) Implement adding of two constraint sets here
        3) Implement all the checks before adding two constraint dicts together with the respective error and warning messages
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.checkValues()

    def __setitem__(self, key, value):
        if isinstance(value, cons_dpq.Constraint):
            super().__setitem__(key, value)
        else:
            raise TypeError(f"Value {value} ({type(value)}) being assigned to key '{key}' is not a queries.constraints_dpqueries.Constraint!")

    def update(self, *args, **kwargs):
        super().update(*args, **kwargs)
        self.checkValues()

    def setdefault(self, *args, **kwargs):
        super().setdefault(*args, **kwargs)
        self.checkValues()

    def checkValues(self):
        """ Auxiliary function to check that all the dict values are Constraint, and raise an error otherwise"""
        for key, value in self.items():
            if not isinstance(value, cons_dpq.Constraint):
                raise TypeError(f"ConstraintDict[{key}] value {value} ({type(value)}) is not a queries.constraints_dpqueries.Constraint!")


    def __add__(self, other):
        """ Check that the sets of constraints are identical in both addends and add each of the individual constraints"""

        if not isinstance(other, ConstraintDict):
            msg = f"Cannot add a ConstraintDict ({type(self)}) with {other}({type(other)})"
            logging.error(msg)
            raise TypeError(msg)

        if set(self.keys()) != set(other.keys()):
            msg = f"Constraint dicts cannot be added, having different key sets: {set(self.keys())} and {set(other.keys())}"
            logging.error(msg)
            raise ValueError(msg)

        const_sum = ConstraintDict()
        for key in self.keys():
            if not isinstance(self[key], cons_dpq.Constraint):
                msg = f"ConstraintDict value {self[key]} ({type(self[key])}) under key '{key}' is not a queries.constraints_dpqueries.Constraint!"
                logging.error(msg)
                raise TypeError(msg)

            assert key == self[key].name, f"Constraint name '{self[key].name}' does not correspond to ConstraintDict key {key}"

            const_sum[key] = self[key] + other[key]

        return const_sum

    def __radd__(self, other):
        """ Just use regular __add__ to take care of TypeError raising """
        return self.__add__(other)
