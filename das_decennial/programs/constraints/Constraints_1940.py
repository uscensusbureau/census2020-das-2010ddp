"""
Constraints for 1940 schema. Largely inherited from PL94
"""
import programs.constraints.Constraints_PL94 as Constraints_PL94
import programs.schema.schemas.Schema_1940 as Schema_1940

class ConstraintsCreator(Constraints_PL94.ConstraintsCreatorPL94Generic):
    """
    This class is what is used to create constraint objects from what is listed in the config file
    Inputs:
        hist_shape: the shape of the underlying histogram
        invarinats: the list of invaraints (already created)
        constraint_names: the names of the constraints to be created (from the list below)
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
        }

        self.setSchema(Schema_1940.buildSchema)
        self.setHhgqDimsAndCaps()
