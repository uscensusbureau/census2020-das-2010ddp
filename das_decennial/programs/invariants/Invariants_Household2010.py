import programs.invariants.querybase_invariants as querybase_invariants
import programs.schema.schemas.Schema_UnitTable10 as UnitSchema

from constants import HHGQ

class InvariantsCreator(querybase_invariants.AbstractInvariantsCreator):
    """
    The schema here is 
    schema:  sex, age, hisp, race, size, hhtype, elderly, multi
    Histogram shape is: 2, 9, 2, 7, 8, 24, 4, 2
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.invariant_funcs_dict = {
            "tot" : self.tot,
            "gqhh_vect": self.gqhh_vect,
        }
        self.unit_schema = UnitSchema.buildSchema()

    def tot(self):
        self.addToInvariantsDict(self.raw_housing, None, "tot", groupings={0:[[0,1]]})

    def total_pop(self):
        """ Need constraint table that allows computing total pop """
        pass

    def hhgq(self):
        """ Need hhgq vector """
        pass

    def gqhh_vect(self):
        """ These are all units, no summing, no subsetting/groupings"""
        # Putting all the housing units (occupied(0) and vacant(1)) in the first cell
        hhgq_groupings = self.unit_schema.recodes['hhgq_vector']['groupings'][HHGQ]
        self.addToInvariantsDict(self.raw_housing, None, "gqhh_vect", groupings={0: hhgq_groupings})

    
