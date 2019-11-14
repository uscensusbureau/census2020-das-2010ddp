from programs.schema.attributes.abstractattribute import AbstractAttribute
from constants import CC


class HHSizeAttr(AbstractAttribute):

    @staticmethod
    def getName():
        return CC.ATTR_HHSIZE

    @staticmethod
    def getLevels():
        return {
            '0-person household'        : [0],
            '1-person household'        : [1],
            '2-person household'        : [2],
            '3-person household'        : [3],
            '4-person household'        : [4],
            '5-person household'        : [5],
            '6-person household'        : [6],
            '7-or-more-person household': [7]
            }


    @staticmethod
    def recodeSizeOnePerson():
        name = CC.HHSIZE_ONE
        groupings = {
                "1-person household": [1]
            }
        return name, groupings

    @staticmethod
    def recodeSizeVacant():
        name = CC.HHSIZE_VACANT
        groupings = {
                "vacant household": [0]
            }
        return name, groupings

    @staticmethod
    def recodeSizeTwoPersonPlus():
        name = CC.HHSIZE_TWOPLUS
        groupings = {
                "2-or-more-person household": list(range(2,8))
            }
        return name, groupings

    @staticmethod
    def recodeSizeTwoPerson():
        name = CC.HHSIZE_TWO
        groupings = {
                "2-person household": [2]
            }
        return name, groupings

    @staticmethod
    def recodeSizeThreePerson():
        name = CC.HHSIZE_THREE
        groupings = {
                "3-person household": [3]
            }
        return name, groupings

    @staticmethod
    def recodeSizeFourPerson():
        name = CC.HHSIZE_FOUR
        groupings = {
            SIZE: {
                "4-person household" : [4]
            }
        }
        return name, groupings

    @staticmethod
    def recodeSizeNonvacantVect(self):
        name = CC.HHSIZE_NONVACANT_VECT
        size_range = range(1,8)
        sizedict = self.getLevels()[SIZE]
        size_groupings = sk.getSubsetTuples(sizedict, size_range)
        groupings = {
            SIZE: dict(size_groupings)
        }
        return name, groupings

    def recodeSizeNotAloneVect(self):
        name = CC.HHSIZE_NOTALONE_VECT
        size_range = range(2,8)
        sizedict = self.getLevels()[SIZE]
        size_groupings = sk.getSubsetTuples(sizedict, size_range)
        groupings = {
            SIZE: dict(size_groupings)
            # Shouldn't it just be
            # SIZE: {"sizes above 1": size_range}
        }
        return name, groupings

    @staticmethod
    def recodeSizeAloneBinary():
        name = CC.HHSIZE_ALONE_BINARY 
        groupings = {
                "Alone"    : [1],
                "Not alone": list(range(2,8))
            }
        return name, groupings
