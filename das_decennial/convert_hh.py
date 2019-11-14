"""
Simple script for taking existing results from specific experiments in pickled format
and turning them into CSV (pipe-delimited, actually) microdata files instead
"""

import os
import sys
import time
import logging

from configparser import ConfigParser

from programs.writer.mdf2020writer import MDF2020HouseholdWriter, MDF2020PersonWriter, addEmptyAndGQ
from programs.writer.rowtools import makeHistRowsFromMultiSparse
from programs.nodes.nodes import SYN, INVAR, GEOCODE
import programs.das_setup as ds
import programs.schema.schemas.Schema_UnitTable10 as UnitSchema

from das_framework.ctools.s3 import s3open
import constants as C

if 'SPARK_HOME' not in os.environ:
    os.environ['SPARK_HOME'] = '/usr/lib/spark'

# Add the location of shared libraries
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python'))
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python', 'lib', 'py4j-src.zip'))

from pyspark.sql import SparkSession, DataFrame


class DASStub:
    def log_and_print(self, message):
        print(f"ANNOTATE: {message}")
        logging.info("ANNOTATE: " + message)

    def make_bom_only(self, *args, **kwargs):
        pass

    def annotate(self, message, verbose=True):
        if verbose:
            print(f"ANNOTATE: {message}")
        logging.info("ANNOTATE: " + message)

class NonConvertingMDF2020HouseholdWriter(MDF2020HouseholdWriter):
    def transformRDDForSaving(self, rdd):
        """ Transformations before saving """

        schema = self.setup.schema_obj

        def gqtype_recoder(schema_code):
            return UnitSchema.mdf_recode[schema_code]

        def node2SparkRows(node: dict):
            # nodedict = node.toDict((SYN, INVAR, GEOCODE))

            # node already comes as a dict, but let's still clear everything except for SYN, INVAR and GEOCODE.
            nodedict = {SYN: node[SYN], GEOCODE: node[GEOCODE]}
            nodedict[INVAR] = node[INVAR] if INVAR in node else node['_invar']

            households = makeHistRowsFromMultiSparse(nodedict, schema, row_recoder=self.row_recoder)
            units = addEmptyAndGQ(nodedict, schema, households, row_recoder=self.row_recoder,
                                  gqtype_recoder=gqtype_recoder)
            return units

        df: DataFrame = rdd.flatMap(node2SparkRows).toDF()

        df = df.select(self.var_list)
        return df


class NonConvertingMDF2020PersonWriter(MDF2020PersonWriter):
    def transformRDDForSaving(self, rdd):
        """ Transformations before saving """

        schema = self.setup.schema_obj

        def node2SparkRows(node: dict):
            # nodedict = node.toDict((SYN, INVAR, GEOCODE))
            nodedict = {SYN: node[SYN], GEOCODE: node[GEOCODE]}
            persons = makeHistRowsFromMultiSparse(nodedict, schema, row_recoder=self.row_recoder)
            return persons

        df: DataFrame = rdd.flatMap(node2SparkRows).toDF()

        df = df.select(self.var_list)

        return df

# DHC-P Experiment # 1 (default manual strategy


class ExperimentPath:
    def __init__(self, type, folder, sub_folders, runs):
        self.type = type
        self.folder = folder
        self.sub_folders = sub_folders
        self.runs = runs


PERSON = "persons"
HOUSEHOLD = "MDF_UNIT"

# Ugly. Can be done by dynamically by querying S3. 
EXPERIMENTS = [
    ExperimentPath(HOUSEHOLD,
        "s3://uscb-decennial-ite-das/DHC_DemonstrationProduct_Fixed/DHCH_BAK3/",
        [
            "td2"
        ],
        1
     )
]

if __name__ == '__main__':

    print('Beginning of conversion script.')

    spark = SparkSession.builder.getOrCreate()
    files_shipped = False

    logging.basicConfig(filename="convert_hh.log", format="%(asctime)s %(filename)s:%(lineno)d (%(funcName)s) %(message)s")
    for experiment in EXPERIMENTS:
        invar_loaded = True
        print(f'Converting {experiment.type} experiment at: {experiment.folder}')
        for sub_folder in experiment.sub_folders:
            for run_number in range(experiment.runs):
                full_path = f'{experiment.folder}/{sub_folder}/run_000{str(run_number)}'
                config_path = f'{experiment.folder}/{sub_folder}/run_000{str(run_number)}/config.ini'

                print(f'Converting experiment at (full path) {full_path}')
                print(f'Config file located at: {config_path}')

                config_file = s3open(config_path).read()

                print(f'type(config file): {type(config_file)}')
                print(f'config file: {config_file}')

                config = ConfigParser()
                config.read_string(config_file)

                print(f'existing writer section: {str(list(config.items(section=C.WRITER_SECTION)))}')
                output_datafile_name = config.get(C.WRITER_SECTION, C.OUTPUT_DATAFILE_NAME)
                print(f'section:writer, output_datafile_name: {output_datafile_name}')

                output_path = f'{experiment.folder}_unpickled/{sub_folder}/run_000{str(run_number)}'
                full_path = f'{full_path}/{output_datafile_name}/'
                config.set(C.WRITER_SECTION, C.OUTPUT_PATH, output_path)
                config.set(C.WRITER_SECTION, C.S3CAT, '1')
                config.set(C.WRITER_SECTION, C.S3CAT_SUFFIX, '.txt')
                config.set(C.WRITER_SECTION, C.OVERWRITE_FLAG, '0')
                config.set(C.WRITER_SECTION, C.WRITE_METADATA, '1')
                config.set(C.WRITER_SECTION, C.CLASSIFICATION_LEVEL, 'CUI//CENS')
                config.set(C.WRITER_SECTION, C.NUM_PARTS, '5000')

                print(f'modified writer section: {str(list(config.items(section=C.WRITER_SECTION)))}')

                print(f'section:schema: {str(list(config.items(section=C.SCHEMA)))}')

                print(f'Converting experiment at (full path) {full_path} to {output_path}')

                # print(f'str(nodes_dict_rdd.take(1)): {str(nodes_dict_rdd.take(1))}')

                das_stub = DASStub()
                das_stub.t0 = time.time()
                das_stub.output_paths = []

                setup_instance = ds.DASDecennialSetup(config=config, name='setup', das=das_stub)
                if not files_shipped:
                    setup_instance = setup_instance.setup_func()  # This ships files to spark
                    files_shipped = True

                print(f"Reading pickled data: {full_path}")

                nodes_dict_rdd = spark.sparkContext.pickleFile(full_path)

                a_node_dict = nodes_dict_rdd.take(1)[0]
                #if not (experiment.type is PERSON):
                #if INVAR not in a_node_dict and '_invar' not in a_node_dict:
                #        if not invar_loaded:
                #            invar_rdd = spark\
                #                .sparkContext\
                #                .pickleFile('') \
                #                .map(lambda nd: (nd[GEOCODE], nd['_invar']))
                #            invar_loaded = True
                #        nodes_dict_rdd = nodes_dict_rdd\
                #            .map(lambda nd: (nd[GEOCODE], nd[SYN]))\
                #            .join(invar_rdd)\
                #            .map(lambda g_sk: {GEOCODE: g_sk[0], SYN: g_sk[1][0], INVAR: g_sk[1][1]})

                # print(nodes_dict_rdd.count())
                # from rdd_like_list import RDDLikeList
                # nodes_dict_rdd = RDDLikeList(nodes_dict_rdd.take(10))

                if experiment.type is PERSON:
                    print('Using Person Writer')
                    w = NonConvertingMDF2020PersonWriter(config=setup_instance.config, setup=setup_instance, name='writer',
                                                         das=das_stub)
                else:
                    print('Using Household Writer')
                    w = NonConvertingMDF2020HouseholdWriter(config=setup_instance.config, setup=setup_instance, name='writer', das=das_stub)

                print('Writing')
                w.write((nodes_dict_rdd, None))
                print(f'Finished Converting experiment at (full path) {full_path}')
        print(f'Finished Converting {experiment.type} experiment at: {experiment.folder}')
    print('End of conversion script.')
