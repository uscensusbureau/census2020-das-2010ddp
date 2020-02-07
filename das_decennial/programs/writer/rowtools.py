from typing import Union, Callable, List

import numpy as np

from pyspark.sql import Row
from programs.nodes.nodes import GeounitNode, getNodeAttr, hasNodeAttr, RAW, SYN, GEOCODE
from programs.schema import schema as sk

def makeHistRowsFromMultiSparse(node: Union[GeounitNode, dict],
                                schema: sk,
                                run_id: int = None,
                                row_recoder: Callable = None,
                                add_schema_name: bool = True,
                                microdata_field: str = 'priv') -> List[Row]:
    """
    Converts a node's raw and/or private histogram into a list of Spark Rows, where columns are the dimensions of the histogram
    and 'priv' and 'orig' are column names containing private and raw counts respectively. If :microdata_field: is set ('priv'/'orig'), then
    instead of having count in that field, the corresponding number (i.e. equal to the count) is inserted in the Row list,
    and the field is discarded.
    :param node: GeounitNode or dict, the node, whose histograms are converted to list or Rows (to then convert to DataFrame)
    :param schema: histogram schema
    :param run_id: auxiliary field, to be keep track of the number of experimental run
    :param row_recoder: recoder class, if some recoding is desired between histogram variables and DataFrame (list of Rows) output columns
    :param add_schema_name: whether to mangle histogram variables name by adding schema name to that, so that they are not confused with desired output names in the recoder
    :param microdata_field: whether to proliferate rows to the histogram count (as opposed to just have a single row for each hist. var. combination with count in a column)
    :return:
    """

    data_present = []  # The list that tells whether the node has raw data, privatized data or both. If yes, the data is in this list.
    if hasNodeAttr(node, RAW):
        orig = getNodeAttr(node, RAW).sparse_array
        data_present.append((orig, 'orig'))
    if hasNodeAttr(node, SYN):
        priv = getNodeAttr(node, SYN).sparse_array
        data_present.append((priv, 'priv'))

    # Collect (multiD) indices of all non-zero histogram counts, for one or both of private/raw datasets
    indices = []
    for dataset in data_present:
        indices = indices + dataset[0].indices.tolist()

    # Leave only unique indices, we will have a Row per each index (each index corresponds to a combination of histogram variables)
    all_nonzero_indices = np.unique(indices)

    rows = []

    if row_recoder is not None:
        recoder = row_recoder()

    # Build the row list
    for ind in all_nonzero_indices:
        rowdict = {}

        # Add run_id
        if run_id is not None:
            rowdict['run_id'] = run_id

        # Add geocode
        rowdict[GEOCODE] = getNodeAttr(node, GEOCODE)

        # And privatized count, or raw count, or both
        for dataset in data_present:
            rowdict[dataset[1]] = int(dataset[0][0, ind])

        # Add all the histogram variables from the schema
        cell = np.unravel_index(ind, schema.shape)
        for dim, level in enumerate(cell):
            dimcol = f"{schema.dimnames[dim]}"
            if add_schema_name:
                dimcol = dimcol + f"_{schema.name}"
            rowdict[dimcol] = str(level)

        # Recode the histogram variables to output variables
        if row_recoder is not None:
            rowdict = recoder.recode(rowdict)

        # Produce microdata (i.e. remove the count column and instead have that number of identical rows)
        if microdata_field is not None:
            # Number of identical rows to add equal to the count (taking either private count in field 'priv', or raw count in field 'orig')
            nrows2add = rowdict[microdata_field]
            # Remove the 'priv' or 'orig' count column, that has been used to calculate number of rows to add
            rowdict.pop(microdata_field, None)
        else:
            nrows2add = 1

        row = Row(**rowdict)
        rows.extend([row] * nrows2add)  # If microdata, then add more than one row, but number of records equal to the count in the histogram

    return rows
