"""
Invariants for DHCP_HHGQ Schema
"""
import programs.invariants.querybase_invariants as querybase_invariants


class InvariantsCreator(querybase_invariants.AbstractInvariantsCreator):
    """
    The person schema here is
    schema:  {'hhgq': 0, 'sex': 1, 'age':2, 'hispanic': 3, 'cenrace': 4, 'citizen':5}
    Person histogram shape is: (8, 2, 116, 2, 63, 2)
    Unit schema is {'hhqg':0}
    Unit histogram size is (8,)
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.invariant_funcs_dict = {
            "tot":          self.tot,
            "gqhh_vect":    self.gqhh_vect,
            "gqhh_tot":     self.gqhh_tot,
            "gq_vect":      self.gq_vect
        }

    def tot(self):
        """ Summing over all axes """
        self.addToInvariantsDict(self.raw, (0, 1, 2, 3, 4, 5), "tot", groupings=None)

    def gqhh_vect(self):
        """ These are all units, no summing, no subsetting/groupings"""
        self.addToInvariantsDict(self.raw_housing, None, "gqhh_vect", groupings=None)

    def gqhh_tot(self):
        """ Summing over all units (only one axis: hhqg), total means no subsetting"""
        self.addToInvariantsDict(self.raw_housing, (0,), "gqhh_tot", groupings=None)

    def gq_vect(self):
        """ Subsetting only GQ (excluding hhgq==0 which means housing unit is not GQ, no summing to do """
        self.addToInvariantsDict(self.raw_housing, (), "gq_vect", groupings={0: [range(1, 8)]})
