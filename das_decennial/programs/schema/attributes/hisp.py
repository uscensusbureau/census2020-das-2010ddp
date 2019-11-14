from programs.schema.attributes.abstractattribute import AbstractAttribute
from constants import CC


class HispAttr(AbstractAttribute):

    @staticmethod
    def getName():
        return CC.ATTR_HISP

    @staticmethod
    def getLevels():
        return {
                'Not Hispanic or Latino': [0],
                'Hispanic or Latino'    : [1]
            }

    @staticmethod
    def recodeHispTotal():
        """
        Subset of the HISP variable to include only the "Hispanic or Latino" category
        """
        name = CC.HISP_TOTAL
        groups = {
                "Hispanic or Latino": [1]
        }
        return name, groups

    @staticmethod
    def recodeNotHispTotal():
        """
        Subset of the HISP variable to include only the "Not Hispanic or Latino" category
        """
        name = CC.HISP_NOT_TOTAL
        groups = {
            "Not Hispanic or Latino": [0]
        }
        return name, groups