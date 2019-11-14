#######################################################
# Block Node Writer Notes
# Updates: 
#   27 February 2019 - bam: Updated to receive, as input, a blocknode rdd instead of a dictionary of rdds
#   16 August 2018 - bam
# 
# How to use block_node_writer in the config file:
#
# Use in the [writer] section of the config file
# writer: programs.block_node_writer.BlockNodeWriter
#
#
# Other attributes and options:
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
# minimize_nodes    : whether or not to call the node's stripForSave function to minimize the memory impact of each node
#                     Use 1 to minimize / 0 to not minimize the nodes
#                     default is 0
#
# num_parts         : indicates how to repartition the rdd for faster saving
#                     default = 100
#
#######################################################
# For quick copying:
#
# [writer]
# writer:
# output_fname:
# produce_flag:
# pickle_batch_size:
# minimize_nodes:
# num_parts:
#
#######################################################

import logging

from programs.writer.writer import DASDecennialWriter
import das_utils

from constants import PICKLE_BATCH_SIZE, MINIMIZE_NODES


## SLG:
## Note sure where this came from
##        if produce:
##            try:
##                print("Calling rdd.count()")
##                rdd.count()
##                start_time = time.time()
##                # rdd = rdd.repartition(num_parts)
##                rdd = rdd.coalesce(num_parts).persist()
##                print(f"With num_parts {num_parts}, time for repartition/coalesce (transformation, should be ~zero): ", time.time() - start_time)
##                saveRunData(saveloc, config=config, feas_dict=feas_dict, rdd=rdd, batch_size=batch_size)
##                print(f"With num_parts {num_parts}, time for saveRunData (action, should be >>0): ", time.time() - start_time)
##            except KeyError:
##                logging.error("Cannot save the pickled RDD due to a KeyError.")
##        
##        return original_rdd

class BlockNodeWriter(DASDecennialWriter):
    """ Writer class which saves all bottom-level (block) geonodes as a pickled RDD of GeounitNode class"""
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.s3cat = False
        self.write_metadata = False

    def transformRDDForSaving(self, rdd):
        """ Transformations before saving. Remove unneeded attributes. """
        if self.getboolean(MINIMIZE_NODES, default=False):
            rdd = rdd.map(lambda node: node.stripForSave()).persist()
        return rdd

    def saveRDD(self, path, rdd):
        """Saves the RDD which is already coalesced to appropriate number of parts and transformed"""
        batch_size = self.getint(PICKLE_BATCH_SIZE, default=10)
        logging.debug("Pickle Batch Size: {}".format(batch_size))
        logging.debug("Saving data to directory: {}".format(path))
        das_utils.savePickledRDD(path, rdd, batch_size=batch_size)
