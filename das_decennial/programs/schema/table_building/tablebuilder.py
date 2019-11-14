import numpy as np
import pandas as pd
import das_utils
from constants import *
import re

def testTableDefs(schema,tables):
    """
    Function searches the dimensions of the tables to show cases where
    the same dimension is used in multiple recodes. The output is printed,
    and this function is useful for developing the table code.
    Use it if you get an error about merging the same dimension when
    running getTableBuilder().
    """
    for table in tables.keys():

        for elmnt in range(0, len(tables[table])):
            # print(table)
            # print(elmnt)
            dims = tables[table][elmnt].split(" * ")

            dims = [schema.getQuerySeed(dim).keepdims[0] for dim in dims if len(schema.getQuerySeed(dim).keepdims) >= 1]
            for dim in np.unique(dims):
                if dims.count(dim) > 1:
                    print(f"Table {table}, element{elmnt} contains rows defined by same dimension {dim} ")
    print("Finished checking table dimensions.")

class TableBuilder():
    def __init__(self, schema, tabledict):
        self.tabledict = tabledict
        
        self.tablenames = list(self.tabledict.keys())
        
        self.workload_querynames = []
        for queries in self.tabledict.values():
            self.workload_querynames += queries
        self.workload_querynames = np.unique(self.workload_querynames).tolist()
        self.workload_querynames.sort(key=lambda k: len(re.split(SCHEMA_CROSS_SPLIT_DELIM, k)))
        
        self.schema = schema
        
        #self.workload_queries = self.schema.getQueries(self.workload_querynames)

    def __repr__(self):
        items = [f"Tables: {self.tablenames}"]
        return "\n".join(items)

    def tableExists(self, tablename):
        if tablename in self.tabledict:
            exists = True
        else:
            exists = False
        return exists
    
    # def getTables(self, tablenames=None, data=None):
    #     if tablenames is None:
    #         tablenames = self.tablenames
    #     else:
    #         tablenames = das_utils.aslist(tablenames)
        
    #     tables = {}
    #     for name in tablenames:
    #         tables[name] = self.getTable(name, data)
        
    #     return tables

    def getCustomTable(self, querynames, data=None):
        querynames = das_utils.aslist(querynames)
        if data is None:
            data = np.zeros(self.schema.shape)
        querynames = self.getCustomQuerynames(querynames)
        return getTable(data, self.schema, querynames).toDF()
        
    def getTable(self, querynames, data=None):
        return self.getCustomTable(querynames, data=data)

    def getTableQueries(self, tablename):
        return self.schema.getQueries(self.getTableWorkload(tablename))

    def getTableWorkload(self, tablename):
        return self.tabledict[tablename]
    
    # def getTable(self, tablename, data=None):
    #     if data is None:
    #         data = np.ones(self.schema.shape)
        
    #     assert tablename in self.tabledict, f"Table '{tablename}' does not exist in this workload."
    #     return getTable(data, self.schema, self.tabledict[tablename]).toDF()
    
    def getWorkload(self):
        return self.workload_querynames
    
    def getWorkloadByTable(self, tablenames=None):
        if tablenames is None:
            tablenames = self.tablenames
        else:
            tablenames = das_utils.aslist(tablenames)
        
        querynames = []
        for name in tablenames:
            if name in self.tabledict:
                querynames += self.tabledict[name]
        
        querynames = np.unique(querynames).tolist()
        
        return querynames
        
    def getCustomQuerynames(self, querynames):
        queries = []
        querynames = das_utils.aslist(querynames)
        for name in querynames:
            if name in self.tabledict:
                queries += self.tabledict[name]
            else:
                if self.schema.isValidQuery(name):
                    queries += [name]
        
        queries = np.unique(queries).tolist()
        return queries
                
            


class Table():
    def __init__(self, answerdict, leveldict):
        self.answerdict = answerdict
        self.queries = list(answerdict.keys())
        self.shapedict = {}
        self.flatdict = {}
        for n,a in answerdict.items():
            self.shapedict[n] = a.shape
            self.flatdict[n] = a.flatten()
        
        self.leveldict = leveldict
    
    def __repr__(self):
        namelen = [len(x) for x in self.queries]
        padding = [max(namelen) - x for x in namelen]
        queries = [f"{query}{' '*padding[i]} | {self.shapedict[query]}" for i,query in enumerate(self.queries)]
        items = [
            "Queries | Shape"
        ]
        items += queries
        return "\n".join(items)
    
    def toDF(self):
        rows = []
        for query in self.queries:
            ans = self.answerdict[query]
            lev = self.leveldict[query]
            for ind in np.ndindex(ans.shape):
                if ind == ():
                    rows.append([query, ind, ans[ind], lev.tolist().pop()])
                else:
                    rows.append([query, ind, ans[ind], lev[ind]])
        
        queries, indices, answers, levels = [list(x) for x in zip(*rows)]
        tab = {
            "Query": queries,
            #"Cell": indices,
            "Answer": answers,
            "Level": levels
        }
        return pd.DataFrame(tab)



def getTable(data, schema, querynames):
    querynames = das_utils.aslist(querynames)
    answerdict = {}
    leveldict = {}
    for name in querynames:
        #print(name)
        answerdict[name] = schema.getQuery(name).answerWithShape(data)
        leveldict[name] = schema.getQueryLevel(name, flatten=False)
    
    return Table(answerdict, leveldict)

