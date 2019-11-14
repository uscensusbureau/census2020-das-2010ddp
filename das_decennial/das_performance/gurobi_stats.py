#!/usr/bin/evn python3
"""
gurobi_stats package.

Reads the information in S3 and produces both tables and graphs
"""


# import xml.etree.ElementTree as ET
# import datetime
# import dateutil.parser
import re
# import os
import sys

# Be sure we can get to das_framework

# from das_framework.ctools import cspark
# from das_framework.ctools import dconfig
from das_framework.ctools import tydoc
from das_framework.ctools import s3

ANNOTATE_RE = re.compile("ANNOTATE: (.*)")
CONFIG_RE   = re.compile("Config path: (.*)")

# import math
# import io
#
# import pandas as pd
# import numpy as np

from constants import *

MAX_WIDTH = 30

SORT_ORDER=['National','State','County','EnumDist','Tract_Group', 'Tract','Block_Group','Block']

import matplotlib
from matplotlib.figure import Figure
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas


class GurobiStats:
    def __init__(self, stats_file, start, end):
        self.stats_file = stats_file
        self.start      = start
        self.end        = end

    def add_explaination(self,doc):
        doc.h1("Legend")
        doc.p("The following information is stored for every optimizer statistics record:")
        t = doc.table()
        t.add_head(['var','description'])
        t.add_data_array([['point','0=gurobi environment created; 1=presolve start; 2=presolve end; 3=optimize end; 10=failsafe start; 11=failsafe end'],
                          ['t', 'The time the statistic was created'],
                          ['t_presolve', 'The time that presolve was called'],
                          ['t_optimize', 'The time that optimize was called'],
                          ['childGeocode', 'Geocode of the children in the statistics'],
                          ['failsafe_invoked', 'Whether or not the failsafe was invoked'],
                          ['parentGeocode', 'Geocode of the parent of the statistics node'],
                          ['stack', 'The callstack when the statistics was recorded'],
                          ['loadavg', 'The 1-minute load average'],
                          ['utime', 'Elapsed user time'],
                          ['stime', 'Elapsed system time'],
                          ['maxrss', 'Process maximum RSS'],
                          ['utime_children', 'utime of the process children'],
                          ['stime_children', 'stime of the process children'],
                          ['maxrss_children', 'Process children maximum RSS']])


    def add_gurobi_report(self,*,doc):
        """Add the gurobi report to the provided doc"""

        if not s3.s3exists(self.stats_file):
            print("S3 file {} does not exist".format(self.stats_file))
            return

        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()

        print("GurobiStats: Spark Reading from {}".format(self.stats_file))
        doc.h1("Run Information")
        doc.p("GurobiStatistics read from {}".format(self.stats_file))

        df = spark.read.json( self.stats_file )
        df.persist()
        df.registerTempTable('gstats')

        # Print the schema
        doc.p("For interactive stats analysis type:")
        doc.pre(f"""
PYSPARK_DRIVER_PYTHON=ipython pyspark
df = spark.read.json('{self.stats_file}')
df.persist().registerTempTable('gstats')
""")
        doc.p("Example query:")
        doc.pre("'select count(*) from gstats').show()")
        doc.p("Describe schema with:")
        doc.pre("spark.sql('desc formatted gstats').show()")
        doc.p("Collected Gurobi Performance Metrics: {:,}".format(df.count()))
        doc.hr()

        ################################################################
        doc.p("Performance metrics schema:")
        t = doc.table()
        t.set_option(tydoc.OPTION_TABLE)
        t.set_caption('Columns in gurobi statistics SparkSQL table')
        t.add_head(['Col Name', 'Data Type', 'Min()', 'Max()'])
        def trim(val):
            s = str(val)
            return s if len(s) < MAX_WIDTH else s[0:MAX_WIDTH]+"..."

        for (col_name,data_type,comment)  in spark.sql("desc formatted gstats").collect():
            row_range = spark.sql(f"select min({col_name}), max({col_name}) from gstats").collect()
            t.add_data( (col_name, data_type, trim(row_range[0][0]), trim(row_range[0][1])) )
        doc.hr()

        
        ################################################################
        doc.p("Optimizer runs and run time by childGeolevel:")
        sortkey="""CASE WHEN childGeolevel='National' then 1
                        WHEN childGeolevel='State' then 2 
                        WHEN childGeolevel='County' then 3
                        WHEN childGeolevel='EnumDist' then 4
                        WHEN childGeolevel='Tract_Group' then 5
                        WHEN childGeolevel='Tract' then 6
                        WHEN childGeolevel='Block_Group' THEN 7
                        WHEN childGeolevel='Block' then 8 
                        ELSE 0 
                   END"""
        cmd = (f"SELECT childGeolevel,failsafe_invoked,point,count(*) as samples, ROUND(MIN(Runtime),4), ROUND(AVG(Runtime),4),"
               f"       ROUND(MAX(Runtime),4), ROUND(STDDEV(Runtime),4) "
               f"       FROM gstats GROUP BY childGeolevel,failsafe_invoked,point ORDER BY {sortkey},failsafe_invoked,point")

        t = doc.table()
        t.set_caption("Optimizer runs and run time (in seconds) by childGeolevel")
        t.add_head('childGeolevel,failsafe_invoked,point,count,min,avg,max,stddev'.split(','))
        for row in spark.sql(cmd).collect():
            t.add_data(row)


        for var in 'NumVars,NumConstrs,NumNZs,NumQNZs,IterCount,BarIterCount'.split(','):
            cmd = (f"SELECT MIN({sortkey}) as sortkey,childGeolevel,count(*),min({var}),round(avg({var}),4),"
                   f"max({var}) FROM gstats WHERE point={CC.POINT_OPTIMIZE_END} GROUP BY childGeolevel ORDER BY sortkey")

            t = doc.table()
            t.set_caption(f"Gurobi {var} by childGeolevel")
            t.add_head(f'childGeolevel,count,min({var}),avg({var}),max({var})'.split(','))
            # Don't add the sort key!
            for row in spark.sql(cmd).collect():
                # And remove the _ from BlockGroup because it will cause LaTeX problems.
                t.add_data(row[1:])

        ################################################################
        ### Now lets do the graph of when the optimizer environments were created
        ### This is how you do a histogram in SQL.  
        ### We know the start and the end of the 
        time_start    = self.start.timestamp()
        total_seconds = (self.end-self.start).total_seconds()
        print("DAS ran from {} for {} seconds".format(time_start,total_seconds))

        fig    = Figure()
        canvas = FigureCanvas(fig)
        ax     = fig.add_subplot(111)

        fig.set_size_inches(12,9)

        #ax.set_ylim(bottom=0,top=1.0)
        ax.set_ylabel('% of geounits')
        #ax.set_xlim(left=0,right=total_seconds)
        ax.set_xlabel('time (s)')

        ### Create a histogram with 1000 bins for each geolevel and optimization point
        ###

        cmd =  (f"SELECT childGeolevel,point,INT((t-{time_start})*1000/{total_seconds}) AS bin,count(*) FROM gstats "
                "GROUP BY childGeolevel,point,bin order by 3")
        data = spark.sql(cmd).collect()

        # Quickly print the data for debugging
        debugging = False
        if debugging:
            dtable = tydoc.tytable()
            dtable.add_head(['childGeolevel','bin','count'])
            [dtable.add_data(row) for row in data]
            dtable.render(sys.stdout,format=tydoc.FORMAT_MARKDOWN)

        # if we have a geolevel not in SORT_ORDER, add it.
        for childGeolevel in set([row[0] for row in data]):
            if childGeolevel not in SORT_ORDER:
                SORT_ORDER.append(childGeolevel)

        # Now plot each of the childGeolevels, for each of the points, in order
        saturations = {CC.POINT_ENV_CREATE:0.1,
                       CC.POINT_PRESOLVE_START:0.3,
                       CC.POINT_PRESOLVE_END:0.6,
                       CC.POINT_OPTIMIZE_END:1.0}


        markers = {CC.POINT_ENV_CREATE:'.',         # point
                       CC.POINT_PRESOLVE_START:'4', # tri_right
                       CC.POINT_PRESOLVE_END:'x',   # x
                       CC.POINT_OPTIMIZE_END:'+'}

        hues = {'National':0,
                'State':0.2,
                'County':0.4,
                'Enumeration District':0.5,
                'Tract_Group': 0.5,
                'Tract':0.6,
                'Block_Group':0.8,
                'Block':0.9}

        import matplotlib
        import matplotlib.colors
        legends=[]

        def filtered_data(childGeolevel,point):
            """Returns a list of childGeolevel,point,bin,count"""
            return filter(lambda datum:datum[0]==childGeolevel and datum[1]==point, data)

        for childGeolevel in SORT_ORDER:
            # For each level, figure out the total, which is the OPTIMIZE_END plus FAILSAFE_END, I think
            total_optimized = max( [0] + [datum[3] for datum in filtered_data(childGeolevel,CC.POINT_OPTIMIZE_END)])
            total_failsafe  = max( [0] + [datum[3] for datum in filtered_data(childGeolevel,CC.POINT_FAILSAFE_END)])

            for point in [CC.POINT_ENV_CREATE,CC.POINT_PRESOLVE_START,CC.POINT_PRESOLVE_END,CC.POINT_OPTIMIZE_END]:
                x = [0]
                y = [0]
                total = 0
                for (childGeolevel_,point_,bin_,count_) in filtered_data(childGeolevel, point):
                    t = bin_ * total_seconds / 1000.0
                    total += count_
                    x.append(t)
                    y.append(total)
                x.append(total_seconds)
                y.append(total)

                if total>0:
                    pointName = CC.POINT_NAMES[point]
                    yvals = [val/total for val in y]
                    color = matplotlib.colors.hsv_to_rgb([ hues[childGeolevel], saturations[point], 0.5])
                    label = f'→{childGeolevel} {pointName}'
                    print("childGeolevel: {} point: {} h: {}  sat: {} color: {} ".format(
                        childGeolevel, point, hues[childGeolevel], saturations[point], color))
                    v = ax.plot( x, yvals, linestyle='-', marker=markers[point], label=label, color=color)
                    if point==CC.POINT_ENV_CREATE:
                        legends.append(v)
        ax.legend(loc='best', fontsize=12)
        doc.append_matplotlib(fig)
        

        ################################################################
        ### Error report
        ### Note: We are having problems when no stack is present in a value.
        """        
        cmd = ("SELECT count(*) as error_count,childGeolevel,error,error_message from gstats WHERE error!='' "
               "GROUP BY error,childGeolevel,error_message,stack ORDER BY error_count ")
        errors = spark.sql(cmd).collect()
        if errors:
            t = doc.table()
            t.set_caption("Errors")
            t.add_head('count,childGeolevel,error,error_message'.split(','))

            # Stack is a stacktrace with each level separated by a STATISTICS_PATHNAME_DELIMITER,
            # so split it and just print the count, error and error_message at the top oevel

            for (count,childGeolevel,error,error_message,stack) in errors:
                for (level,part) in enumerate(stack.split(STATISTICS_PATHNAME_DELIMITER)):
                    if 'pyspark.zip' in part: # stop when we get to the zip file
                        break
                    loc = part.find("programs/")
                    if loc>0:
                        part = part[loc:]
                    if level==0:
                        # If we have  programs/ directory, remove things beofre it
                        # And remove the _ from BlockGroup because it will cause LaTeX problems.
                        t.add_data( (count, childGeolevel, error, error_message, part) )
                    else:
                        t.add_data( ('', '', '', '', part) )
                    
        else:
            doc.p("No errors!")

        self.add_explaination(doc)
        """
