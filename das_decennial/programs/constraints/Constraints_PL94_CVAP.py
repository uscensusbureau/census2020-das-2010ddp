"""
Constraints for PL94_CVAP schema.
ConstraintCreator class is an actual PL94_CVAP constraint creator.

ConstraintCreator inherits from ConstraintsCreatorPL94Generic the actual methods creating constraints,
but has its own __init__
"""
import programs.constraints.Constraints_PL94 as Constraints_PL94
import programs.schema.schemas.Schema_PL94_CVAP as Schema_PL94_CVAP


class ConstraintsCreator(Constraints_PL94.ConstraintsCreatorPL94Generic):
    """
        This class is what is used to create constraint objects from what is listed in the config file, for PL94_CVAP schema
        see base class for keyword arguments description

        The variables in PL94 are: hhgq voting hispanic cenrace ( schema:  {'hhgq': 0, 'voting': 1, 'hispanic': 2, 'cenrace': 3, 'citizen': 4} )
        With ranges: 0-7, 0-1, 0-1, 0-62, 0-1 respectively
        Thus the histograms are 4-dimensional, with dimensions (8,2,2,63,2)
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.constraint_funcs_dict = {
            "total":         self.total,
            "hhgq_total_lb": self.hhgq_total_lb,
            "hhgq_total_ub": self.hhgq_total_ub,
            "voting_age":    self.voting_age,
            "hhgq_va_ub":    self.hhgq_va_ub,
            "hhgq_va_lb":    self.hhgq_va_lb,
            "nurse_nva_0":   self.nurse_nva_0
        }

        self.setSchema(Schema_PL94_CVAP.buildSchema)
        self.setHhgqDimsAndCaps()
