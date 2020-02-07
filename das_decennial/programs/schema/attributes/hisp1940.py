from programs.schema.attributes.abstractattribute import AbstractAttribute
from constants import CC


class Hispanic1940Attr(AbstractAttribute):

    @staticmethod
    def getName():
        return CC.ATTR_HISPANIC_1940

    @staticmethod
    def getLevels():
        return {
            'Not Hispanic': [0],
            'Hispanic': [1]
        }

    @staticmethod
    def recodeHispTotal():
        """
        Subset of the HISP variable to include only the "Hispanic" category
        """
        name = CC.HISP_TOTAL
        groups = {
            "Hispanic": [1]
        }
        return name, groups

    @staticmethod
    def recodeNotHispTotal():
        """
        Subset of the HISP variable to include only the "Not Hispanic or Latino" category
        """
        name = CC.HISP_NOT_TOTAL
        groups = {
            "Not Hispanic": [0]
        }
        return name, groups
