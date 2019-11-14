from abc import abstractmethod, ABCMeta
import programs.schema.schema as sk


class SchemaFactory(metaclass=ABCMeta):

    def __init__(self, name, dimnames, shape, path=None):

        levels = sk.unravelLevels(self.getLevels())

        ###############################################################################
        # Construct the schema object
        ###############################################################################
        self.schema = sk.Schema(name, dimnames, shape, recodes=None, levels=levels)

        self.addRecodes()

        ###############################################################################
        # Save Schema (as a JSON file)
        ###############################################################################
        if path is not None:
            self.schema.saveAsJSON(path)

    def getSchema(self):
        return self.schema

    @staticmethod
    @abstractmethod
    def getLevels():
        pass

    @abstractmethod
    def addRecodes(self):
        """
        Need the following information to build a recoded variable

        name = "name of recode"       # try to use python variable and function naming conventions for the names of recodes
        groupings = {
            'one of the dimname variables': {
               'grouping_level_0': [list of indices in grouping_level_0],
               'grouping_level_1': [list of indices in grouping_level_1],
               ...
            },
            'another dimname variable': { ... }, ...
        }
        schema.addRecode(name, groupings)

        Note that it's common practice to recode only one dimension at a time because crossing two recoded (non-overlapping) variables
        is the same as creating a single recode of the two (non-overlapping) variables, but with the benefit of being able to cross the
        individually-recoded variables with any other (non-overlapping) variable.

        Example:
            householder:
            >>> schema.addRecode("householder", { HHGQ: { "Householder" : [0] }})
            voting:
            >>> schema.addRecode("voting", {VOTING_AGE: { "Voting Age": [1] }})
            hhvoting:
            >>> schema.addRecode("hhvoting", { HHGQ: { "Householder": [0] }, VOTING_AGE: { "Voting Age": [1] } })

        "householder * voting" is the same as "hhvoting", but householder can also be easily crossed with, say, "race" to form "householder * race",
        which wouldn't be possible if using only hhvoting, since "hhvoting * race" is the same as saying "householder * voting * race".

        These two recode the same thing, but the following shows how to recode concisely (for simple recodes) and the one after shows how to use functions
        to generate recodes (for longer or more complex ones)
        >>> schema.addRecode("voting", { VOTING_AGE: { "Voting Age": [1] }})
        """
        pass