class _SchemaConst:
    def __setattr__(self, name, value):
        raise TypeError(f"Attempt to set read/only constant {name}")

    TOTAL = "total"
    DETAILED = "detailed"
    HHGQ = "hhgq"
    SEX = "sex"
    HISP = "hispanic"
    CENRACE = "cenrace"
    RACE = "race"
    AGE = "age"
    AGECAT = "agecat"
    CITIZEN = "citizen"
    REL = "rel"

