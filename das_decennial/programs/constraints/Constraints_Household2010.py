import numpy as np
import programs.constraints.querybase_constraints as querybase_constraints
import programs.schema.schemas.Schema_Household2010 as Schema_Household2010




class ConstraintsCreatorHousehold2010Generic(querybase_constraints.AbstractConstraintsCreator):


    def total(self, name):
        """
        Total number of households is less than total number of housing units
        """
        self.checkInvariantPresence(name, ('tot',))
        self.addToConstraintsDict(name, "total", self.invariants["tot"].astype(int), "le")


    # Structural zeros

    # size related.
    def no_vacant(self, name):
        """ size=0 cannot happen """
        self.addToConstraintsDict(name, "vacant", np.array(0), "=")

    def living_alone(self, name):
        """
        if size = 1, hhtype=18 and multi=0
            age<5 iff elderly=0
            age=5 iff elderly=1
            age=6 iff elderly=2
            age>6 iff elderly=3
        """
        self.addToConstraintsDict(f"{name}_gt1", "size1 * invert_alone", np.array(0), "=") # rules out all hhtype but living alone
        self.addToConstraintsDict(f"{name}_multi", "size1 * yes_multi", np.array(0), "=") # rules out multigenerational household
        self.addToConstraintsDict(f"{name}_eld0", "size1 * alone * no_multi * age_under60 * invert_elderly0", np.array(0), "=") # rules out hh under 60 and presence of 60+
        self.addToConstraintsDict(f"{name}_eld1", "size1 * alone * no_multi * age_60to64 * invert_elderly1", np.array(0), "=")  # rules out hh between 60 to 64 and (presence of 65+ or no presence of 60+)
        self.addToConstraintsDict(f"{name}_eld2", "size1 * alone * no_multi * age_65to74 * invert_elderly2", np.array(0), "=")  # similar for 65 to 74
        self.addToConstraintsDict(f"{name}_eld3", "size1 * alone * no_multi * age_75plus * invert_elderly3", np.array(0), "=")  # similar for 75+


    def size2(self, name):
        r"""
        if size=2, hhtype \in {3,7,12,17,19,20,22,23} (other types imply at least 3 people in hhd) and multi=0
            if age<5 and elderly>0, hhtype is not 19 or 20
            if age=0 and elderly=3, hhtype must be 22 or 23
            if the second person is a child, elderly completely deterimined by hh age
        """
        self.addToConstraintsDict(f"{name}_gt2", "size2 * non2type", np.array(0), "=") # rules out living alone and all types that imply at least 3 people
        self.addToConstraintsDict(f"{name}_multi", "size2 * yes_multi", np.array(0), "=") # rules out multigenerational households
        self.addToConstraintsDict(f"{name}_eld0", "size2 * type2_onechild * age_under60 * invert_elderly0", np.array(0), "=") # rules out hh under 60 and presence of 60+ and child under 18.
        self.addToConstraintsDict(f"{name}_eld1", "size2 * type2_onechild * age_60to64 * invert_elderly1", np.array(0), "=") # similar
        self.addToConstraintsDict(f"{name}_eld2", "size2 * type2_onechild * age_65to74 * invert_elderly2", np.array(0), "=") # similar
        self.addToConstraintsDict(f"{name}_eld3", "size2 * type2_onechild * age_75plus * invert_elderly3", np.array(0), "=") # similar
        self.addToConstraintsDict(f"{name}_le25_eld3", "size2 * age_15to24 * elderly3 * type2_couple", np.array(0), "=") # rules out hh under 25 and presence of 75+ and married/partner.

    def size3(self, name):
        r"""
        if size=3, hhtype \notin {2,6,10,15,18}
           if hhtype=21, must be householder with 2 children. multi=0, elderly completely detemined by hh age.
           if multi=1, must be hhtype \in {19, 20, 22}
        """
        #self.addToConstraintsDict(name, "size3 * yes_multi * type3_child_under_6 * ", np.array(0), "=") # multi gen plus child under 6 implies no grandchild. False not struct zero grandchild could be child of another child not living in house.
        self.addToConstraintsDict(f"{name}_le25_eld3", "size3 * type3_couple_with_child * age_15to24 * elderly3", np.array(0), "=")
        self.addToConstraintsDict(f"{name}_gt3", "size3 * non3type", np.array(0), "=") # rules out living alone and all types that imply at least 4 people
        self.addToConstraintsDict(f"{name}_multi", "size3 * yes_multi * type3_notmulti", np.array(0), "=") # rules out multigenerational and householder has spouse/partner.
        self.addToConstraintsDict(f"{name}_eld0", "size3 * type3_twochild * age_under60 * invert_elderly0", np.array(0), "=") # rules out hh under 60 and presence of 60+ and 2 children under 18.
        self.addToConstraintsDict(f"{name}_eld1", "size3 * type3_twochild * age_60to64 * invert_elderly1", np.array(0), "=") # similar
        self.addToConstraintsDict(f"{name}_eld2", "size3 * type3_twochild * age_65to74 * invert_elderly2", np.array(0), "=") # similar
        self.addToConstraintsDict(f"{name}_eld3", "size3 * type3_twochild * age_75plus * invert_elderly3", np.array(0), "=") # similar


    def size4(self, name):
        """
        Is there anything special? oh yes.
        if hhtype=2, must be spouse, 2 children so multi=0.
        """
        self.addToConstraintsDict(f"{name}_gt4", "size4 * alone", np.array(0), "=")
        self.addToConstraintsDict(f"{name}_multi", "size4 * type4_notmulti * yes_multi", np.array(0), "=")
        self.addToConstraintsDict(f"{name}_le25_eld3", "size4 * type4_couple_with_twochildren * age_15to24 *elderly3", np.array(0), "=")



    def size2plus_notalone(self, name):
        """
        Can't be living alone if more than two people
        """
        self.addToConstraintsDict(f"{name}_gt2plus", "size2plus * alone", np.array(0), "=")


    def not_multigen(self, name):
        """
        Household is multigen if the householder has both a child and a parent/parent-in-law or both a child and a grandchild.
        Child doesn't have to be under 18 so not many restrictions here.
        if multi=1, then hhtype \notin {12,17,18,23}
        """
        self.addToConstraintsDict(f"{name}_1", "yes_multi * never_multi", np.array(0), "=")


    def age_child(self, name):
        """
        Male householder can't have children more than 69 yrs younger.
        Female householder cant' have children more than 50 yrs younger.
        if sex=male and age>=75, cannot have own children under 6 yrs
        if sex=female and age>=75, cannot have own children under 18 yrs.
        if sex=female and age>=60, cannot have own children under 6yrs.
        """
        #self.addToConstraintsDict(f"{name}_1", "male * age_75plus * ownchild_under6", np.array(0), "=")
        #self.addToConstraintsDict(f"{name}_2", "female * age_75plus * ownchild_under18", np.array(0), "=")
        #self.addToConstraintsDict(f"{name}_3", "female * age_60plus * ownchild_under6", np.array(0), "=")
        pass

    def hh_elderly(self, name):
        """
        In general, householder age partially defines elderly.
        """
        self.addToConstraintsDict(f"{name}_eld3", "age_75plus * invert_elderly3", np.array(0), "=")
        self.addToConstraintsDict(f"{name}_2", "age_65to74 * pres_under65", np.array(0), "=")
        self.addToConstraintsDict(f"{name}_3", "age_60to64 * pres_under60", np.array(0), "=")

    def total_ub(self, name):
        """
        upper bound on total as derived from housing unit invariant
        """
        pass

    def population_ub(self, name):
        """
        sum of size s * number of size s <= pop where ever pop is an invariant
        """
        pass

    def population_lb(self, name):
        pass


class ConstraintsCreator(ConstraintsCreatorHousehold2010Generic):
    """
       This class is what is used to create constraint objects from what is listed in the config file, for household schema
       see base class for keyword arguments description
       Schema: sex, age, hisp, race, size, hhtype, elderly, multi
       Schema dims: 2, 9, 2, 7, 8, 24, 4, 2
       """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.constraint_funcs_dict = {
            "total": self.total,
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

        self.setSchema(Schema_Household2010.buildSchema)