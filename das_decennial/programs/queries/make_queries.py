class QueriesCreator:
    """
    Class to create queries dict from schema by list of names
    """
    def __init__(self, schema_obj, query_names):
        self.schema = schema_obj
        self.query_names = query_names
        self.queries_dict = self.schema.getQueries(self.query_names)
