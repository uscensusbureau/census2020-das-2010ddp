"""
Invariants for 1940 Schema
"""
import programs.invariants.querybase_invariants as querybase_invariants

class InvariantsCreator(querybase_invariants.AbstractInvariantsCreator):
    """ Invariant creator for 1940 public use data """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.invariant_funcs_dict = {
            "tot"                       : self.tot,
            "va"                        : self.va,
            "gqhh_vect"                 : self.gqhh_vect,
            "gqhh_tot"                  : self.gqhh_tot,
            "gq_vect"                   : self.gq_vect
        }

    def tot(self):
        """ Summing over all axes """
        self.addToInvariantsDict(self.raw, (0, 1, 2, 3, 4), "tot", groupings=None)

    def va(self):
        """ Summing over all axes except 'voting' (1) and subsetting axis 1 to value 1 (voting age)"""
        self.addToInvariantsDict(self.raw, (0, 2, 3, 4), "va", groupings={1: [[1]]})

    def gqhh_vect(self):
        """ These are all units, no summing, no subsetting/groupings"""
        self.addToInvariantsDict(self.raw_housing, None, "gqhh_vect", groupings=None)

    def gqhh_tot(self):
        """ Summing over all units (only one axis: hhqg), total means no subsetting"""
        self.addToInvariantsDict(self.raw_housing, (0,), "gqhh_tot", groupings=None)

    def gq_vect(self):
        """ Subsetting only GQ (excluding hhgq==0 which means housing unit is not GQ, no summing to do """
        self.addToInvariantsDict(self.raw_housing, (), "gq_vect", groupings={0: [range(1, 8)]})
