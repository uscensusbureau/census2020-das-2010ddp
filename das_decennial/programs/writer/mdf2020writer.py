"""
mdf2020writer.py:  Writes out the MDF in the 2020 format.
"""
# Pavel Zhuravlev
# 6/4/19

from typing import Union, Callable, List
from pyspark.sql import DataFrame, Row

from programs.writer.writer import DASDecennialWriter
from programs.writer.rowtools import makeHistRowsFromMultiSparse
from programs.writer.dhcp_hhgq_to_mdfpersons2020 import DHCPHHGQToMDFPersons2020Recoder # DHCP HHGQ Recoder (Demonstration Products)
from programs.nodes.nodes import GeounitNode, SYN, INVAR, GEOCODE, getNodeAttr

from das_framework.ctools.s3 import s3open


class MDF2020Writer(DASDecennialWriter):

    var_list: List[str]   # List of MDF columns to write

    def saveHeader(self, *, path):
        """Saves header to the requested S3 location. This header will then be combined with the contents by the s3cat command"""
        self.annotate(f"writing header to {path}")
        with s3open(path, "w", fsync=True) as f:
            f.write("|".join(self.var_list))
            f.write("\n")
        
    def saveRDD(self, path, rdd: DataFrame):
        """
        Saves the RDD which is already coalesced to appropriate number of parts and transformed
        This one saves it either as a single file or splits by state
        Called from writer.py:saveRunData(), which reports that the file is being written, writes the file, 
        then optionally writes the metadata and runs s3cat
        """

        rdd.write.csv(path, sep="|")


class MDF2020PersonWriter(MDF2020Writer):
    """ 
    Applies recodes and saves file for the DHCP_HHGQ Demonstration product
    """
    var_list = [
        "SCHEMA_TYPE_CODE",
        "SCHEMA_BUILD_ID",
        "TABBLKST",
        "TABBLKCOU",
        "TABTRACTCE",
        "TABBLKGRPCE",
        "TABBLK",
        "EPNUM",
        "RTYPE",
        "GQTYPE",
        "RELSHIP",
        "QSEX",
        "QAGE",
        "CENHISP",
        "CENRACE",
        "CITIZEN",
        "LIVE_ALONE"
    ]

    row_recoder = DHCPHHGQToMDFPersons2020Recoder

    def transformRDDForSaving(self, rdd):
        """ Transformations before saving """

        schema = self.setup.schema_obj

        def node2SparkRows(node: GeounitNode):
            nodedict = node.toDict((SYN, INVAR, GEOCODE))
            persons = makeHistRowsFromMultiSparse(nodedict, schema, row_recoder=self.row_recoder)
            return persons

        df: DataFrame = rdd.flatMap(node2SparkRows).toDF()

        df = df.select(self.var_list)

        return df

