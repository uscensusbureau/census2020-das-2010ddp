"""
Constraints for PL94_P12 schema. Largely inherited from PL94
"""
import programs.constraints.Constraints_PL94 as Constraints_PL94
import programs.schema.schemas.Schema_PL94_P12 as Schema_PL94_P12

class ConstraintsCreator(Constraints_PL94.ConstraintsCreatorPL94Generic):
    """
    This class is what is used to create constraint objects from what is listed in the config file
    Inputs:
        hist_shape: the shape of the underlying histogram
        invarinats: the list of invaraints (already created)
        constraint_names: the names of the constraints to be created (from the list below)

    The variables in PL94_P12 are: hhgq sex age_cat hispanic cenrace ( schema:  {'hhgq': 0, 'sex': 1, 'age_cat':2, 'hispanic': 3, 'cenrace': 4}
    With ranges: 0-7, 0-1, 0-22, 0-1, 0-62 respectively
    Thus the histograms are 4-dimensional, with dimensions (8,2,23,2,63)

    All the constraint creation functions are identical to those of PL94 and are inherited
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.constraint_funcs_dict = {
            "total"                     : self.total,
            "hhgq_total_lb"             : self.hhgq_total_lb,
            "hhgq_total_ub"             : self.hhgq_total_ub,
            "voting_age"                : self.voting_age,
            "hhgq_va_ub"                : self.hhgq_va_ub,
            "hhgq_va_lb"                : self.hhgq_va_lb,
            "nurse_nva_0"               : self.nurse_nva_0
        }

        self.setSchema(Schema_PL94_P12.buildSchema)
        self.setHhgqDimsAndCaps()
