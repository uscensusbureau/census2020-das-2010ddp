"""
Invariants for SF1 Schema
"""
import programs.invariants.querybase_invariants as querybase_invariants

class InvariantsCreator(querybase_invariants.AbstractInvariantsCreator):
    """
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.invariant_funcs_dict = {
            "tot"                       : self.tot,
            "gqhh_vect"                 : self.gqhh_vect,
            "gqhh_tot"                  : self.gqhh_tot,
            "gq_vect"                   : self.gq_vect

        }
        self.invariants_dict = {}

    def tot(self):
        """ Summing over all axes """
        self.addToInvariantsDict(self.raw, (0, 1, 2, 3, 4), "tot", groupings=None)

    def gqhh_vect(self):
        """ These are all units, no summing, no subsetting/groupings"""
        # self.addToInvariantsDict(self.raw_housing, None, "gqhh_vect", groupings = None)
        # Putting all the non-GQ in the first cell
        self.addToInvariantsDict(self.raw_housing, None, "gqhh_vect", groupings={0: [range(0, 15)] + [[i] for i in range(15,43)]})

    def gqhh_tot(self):
        """ Summing over all units (only one axis: hhqg), total means no subsetting"""
        self.addToInvariantsDict(self.raw_housing, (0,), "gqhh_tot", groupings=None)

    def gq_vect(self):
        """ Subsetting only GQ"""
        # The (15,17) is likely residual from working with previous schema, leaving it here, in case it's not,
        # as to note lose this edit history
        # self.addToInvariantsDict(self.raw_housing, (), "gq_vect", groupings = {0 : [range(15, 17)]})

        # In SF1 as we have it now (1/25/2019), the dimension of 'rel' axis is 43. The first 15 are relations to householder
        # and the rest are GQ types, whiche are what we want to select here
        self.addToInvariantsDict(self.raw_housing, (), "gq_vect", groupings={0: [range(15, 43)]})
