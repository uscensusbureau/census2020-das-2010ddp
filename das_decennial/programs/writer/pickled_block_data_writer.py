#######################################################
# Pickled Block Data Writer Notes
# Updates:
#   15 November 2018 - bam
#
# How to use pickled_block_data_writer in the config file:
#
# Use in the [writer] section of the config file
# writer: programs.pickled_block_data_writer.PickledBlockDataWriter
#
#
# Other attributes and options:
# keep_attrs        : determines which of the slots attributes of the GeounitNodes
#                     should be retained and placed in the dicts that will replace
#                     the GeounitNode objects
#
# output_fname      : the output path where the data will be stored
#                     automatically detects if s3 path or not
#
# produce_flag      : whether or not to write the data to file
#                     Use 1 to save / 0 to not save the data
#
# pickle_batch_size : changes the batchSize attribute of the RDD.saveAsPickleFile function
#                     only include this if a different batchSize is needed; default is 10
#                     leaving it blank also sets the value to 10
#
# num_parts         : indicates how to repartition the rdd for faster saving
#                     default = 100
#
#######################################################
# For quick copying:
#
# [writer]
# keep_attrs:
# output_fname:
# produce_flag:
# pickle_batch_size:
# num_parts:
#
#######################################################

import logging

from constants import KEEP_ATTRS

from programs.writer.block_node_writer import BlockNodeWriter


class PickledBlockDataWriter(BlockNodeWriter):
    """ Writer class which saves all bottom-level (block) geonodes as a pickled RDD of dicts"""

    def transformRDDForSaving(self, rdd):
        """ Transformations before saving. Keep only requested attributes and convert to dict """
        logging.debug("Transforming node information to dict.")

        keep_attrs = self.gettuple(KEEP_ATTRS, default=None)

        logging.debug("Keeping the following node attributes: {}".format(keep_attrs))
        # transform each node into a dictionary
        rdd = rdd.map(lambda node: node.toDict(keep_attrs))

        return rdd
