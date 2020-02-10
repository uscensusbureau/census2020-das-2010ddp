from programs.reader.table_reader import TableVariable
from programs.reader.sql_spar_table import SQLSparseHistogramTable
import constants as C
import das_utils
import typing

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import Row
    from pyspark.sql.types import StringType, IntegerType
except ImportError as e:
    pass


class FWFTableVariable(TableVariable):
    """
    Fixed Width Format Table Variable
    """
    def __init__(self, name, column, width, **kwargs):
        super().__init__(name, **kwargs)
        self.column = column
        self.width = width


class FixedWidthFormatTable(SQLSparseHistogramTable):
    def make_variables(self) -> typing.List[FWFTableVariable]:
        """
        Dynamically create variables based on the generated specification file
        """
        generated_module = f"{self.name}.generated_module"
        generated_table_name = f"{self.name}.generated_table"
        table_name = self.getconfig(generated_table_name, section=C.READER)
        generated_module = das_utils.class_from_config(self.config, generated_module, C.READER)
        self.generated_class = getattr(generated_module, table_name)
        generated_spec = generated_module.SPEC_DICT["tables"][table_name]

        variables = [FWFTableVariable(spec_var["name"].lower(),
                                      column=spec_var["column"], width=spec_var["width"],
                                      vtype=spec_var["vtype"], legal=spec_var["ranges"])
                     for spec_var in generated_spec["variables"]]

        for var in variables:
            var.set_vtype(var.vtype)
            var.set_legal_values_from_ranges(var.legal_values)
        return variables

    def load(self, filter=None):
        """
        Load the fixed-width formatted file, using the generated specification file to parse rows.
        Can optionally apply a filter to select a subset of the

        :param filter: A function to be applied to the RDD after reading in the FWF file. This will subset the rows based on
                       the result of the filter function. Defaults to None, meaning that all rows are loaded.

        :return: A DataFrame with rows corresponding to the specification file
        """
        self.annotate(f'In FWF Reader load: {self.location}')
        spark = SparkSession.builder.getOrCreate()
        rdd = spark.sparkContext.textFile(self.location[0])

        if filter is not None:
            rdd = rdd.filter(filter)

        rdd = rdd.map(lambda line: self.generated_class.parse_line(line))
        rdd = spark.createDataFrame(rdd)

        return rdd


