import programs.invariants.querybase_invariants as querybase_invariants


class InvariantsCreator(querybase_invariants.AbstractInvariantsCreator):
    """
    The schema here is 
    schema:  sex, age, hisp, race, size, hhtype, elderly, multi
    Histogram shape is: 2, 9, 2, 7, 8, 24, 4, 2
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.invariant_funcs_dict = {
            "tot" : self.tot
        }

    def tot(self):
        self.addToInvariantsDict(self.raw_housing, (), "tot", groupings={0:[[0,1]]})

    def total_pop(self):
        """ Need constraint table that allows computing total pop """
        pass

    def hhgq(self):
        """ Need hhgq vector """
        pass

    
