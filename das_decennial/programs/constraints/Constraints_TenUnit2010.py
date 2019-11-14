import numpy as np
import programs.constraints.querybase_constraints as querybase_constraints
import programs.constraints.Constraints_Household2010
import programs.schema.schemas.Schema_TenUnit2010 as Schema_TenUnit2010

class ConstraintsCreator(programs.constraints.Constraints_Household2010.ConstraintsCreatorHousehold2010Generic):
    """
    This class is what is used to create constraint objects from what is listed in the config file, for household schema
    see base class for keyword arguments description
    Schema: sex, age, hisp, race, size, hhtype, elderly, multi, tenure
    Schema dims: 2, 9, 2, 7, 8, 24, 4, 2, 4
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.constraint_funcs_dict = {
            "total" : self.total,
            "no_vacant": self.no_vacant,
            "living_alone": self.living_alone,
            "size2": self.size2,
            "size3": self.size3,
            "size4": self.size4,
            "size2plus_notalone": self.size2plus_notalone,
            "not_multigen": self.not_multigen,
            "hh_elderly": self.hh_elderly,
            "age_child": self.age_child
        }

        self.setSchema(Schema_TenUnit2010.buildSchema)