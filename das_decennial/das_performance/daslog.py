#!/usr/bin/evn python3
"""DAS Logfile Decoder.
This module reads the logfiles created by the DAS and converts them into data objects.
It also has a __main__ program for testing the logfile reader.
"""

import xml.etree.ElementTree as ET
import datetime
import dateutil.parser
import re 
import os
import sys

# This is to support standalone operation
try:
    import das_framework
    import das_stats
except ImportError:
    sys.path.append( os.path.dirname(__file__))
    sys.path.append( os.path.join(os.path.dirname(__file__), ".."))



from das_framework.ctools import cspark
from das_framework.ctools import dconfig
from das_framework.ctools import tydoc


ANNOTATE_RE = re.compile("ANNOTATE: (.*)")
CONFIG_RE   = re.compile("Config path: (.*)")

from constants import *

class DASLogfile:
    def __init__(self,daslog=None,dfxml=None):
        self.annotations = []   # array of (datetime,message)
        self.config_file = ""
        if daslog and not dfxml:
            dfxml_potential = daslog.replace(".log",".dfxml")
            if os.path.exists(dfxml_potential):
                dfxml = dfxml_potential
            else:
                # Find the DFXML file in the daslog
                # We use dopen so we can open S3 files or local files
                with dconfig.dopen(daslog,"r") as f:
                    for line in f:
                        if "aws s3 cp" in line:
                            s3loc = [word for word in line.split(" ") if word.startswith("s3:")][0]
                            print("s3loc:",s3loc)
                            print("Please download the DFXML file from ",s3loc," and use it")
                            exit(1)

        if not dfxml:
            raise FileNotFoundError("dfxml logfile required")
        self.tree       = ET.fromstring( dconfig.dopen(dfxml,"r").read())
        self.first_date = dateutil.parser.parse( self.tree.find(".//start_time").text)
        self.runtime    = float(self.tree.find(".//elapsed_seconds").text)
        self.last_date  = self.first_date + datetime.timedelta(seconds = self.runtime)
        self.dfxml_filename = dfxml
        self.log_filename   = dfxml.replace(".dfxml",".log")
        self.sparkdoc   = self.tree.find(".//spark")
        if self.sparkdoc:
            self.appId       = self.sparkdoc.find("application").attrib['id']
        else:
            self.appId  = None
        self.timestamps = self.tree.findall(".//timestamp")
        
    def prettyprint(self):
        import xml.dom.minidom
        print(xml.dom.minidom.parse(dl.dfxml_filename).toprettyxml(indent='  '))
            
    def gurobi_stats_file(self):
        try:
            return [stat.attrib['filename'] for stat in self.tree.findall(".//gurobiStatistics")][0]
        except IndexError:
            return None

    def clusterId(self):
        """Return the clusterId"""
        for var in self.tree.findall(".//var"):
            if var.attrib['name']=='CLUSTERID':
                return var.attrib['value']
        raise RuntimeError("No CLUSTERID in DFXML <vars>")

    
    def addTimestamps(self,doc):
        t = doc.table()
        t.set_caption("Timestamps")
        for val in self.timestamps:
            t.add_data([str(round(float(val.attrib['total']))), val.attrib['name']])
        t.set_option(tydoc.OPTION_LONGTABLE)

    def addSparkExecutionReport(self,doc):
        """Add the spark execution report to the HTML doc"""

        def toTimestamp(sparkTime):
            return dateutil.parser.parse(sparkTime).timestamp()

        ret = ''

        IGNORED_SPARK_PARAMETERS = ['endTime','lastUpdated''duration','sparkUser','completed',
                                    'endTimeEpoch','lastUpdatedEpoch']

        t = doc.table()
        t.set_option(tydoc.OPTION_LONGTABLE)
        t.set_caption("Spark parameters")
        t.set_fontsize(8)

        for val in self.sparkdoc.find("attempt/dict"):
            if val.tag not in IGNORED_SPARK_PARAMETERS:
                t.add_data([val.tag, val.text])

        # Collect all of the jobs into an array, then sort it.
        # In my tests it is in reverse order, but this is not guarenteed
        t = doc.table()
        t.set_caption("Spark jobs")
        t.set_option(tydoc.OPTION_LONGTABLE)
        sparkStartTime = toTimestamp(self.sparkdoc.find("attempt/dict/startTime").text)
        rows = []
        
        for job in self.sparkdoc.findall("jobs/list/dict"):
            submissionTime = toTimestamp(job.find("submissionTime").text)
            submissionTimeStr = "{:.1f}".format(submissionTime - sparkStartTime)
            try:
                completionTime = toTimestamp(job.find("completionTime").text)
                completionTimeStr = "{:.1f}".format(completionTime - submissionTime)
            except AttributeError as e:
                completionTimeStr = 'n/a'

            name = job.find("name").text
            name = os.path.basename(name)
            
            row = [int(job.find("jobId").text),
                   name,
                   submissionTimeStr,
                   completionTimeStr,
                   job.find("status").text,
                   job.find("numTasks").text,
                   job.find("numCompletedTasks").text,
                   job.find("numSkippedTasks").text,
                   job.find("numFailedTasks").text,
                   job.find("numKilledTasks").text]
            rows.append(row)
        rows.sort()
        t.add_head(["jobID","name","start","time","status","tasks","completed","skipped","failed","killed"])
        for row in rows:
            t.add_data(row)

        t = doc.table()
        t.set_option(tydoc.OPTION_LONGTABLE)
        params = ['id','rddBlocks','memoryUsed','diskUsed','totalCores','maxTasks','completedTasks',
                  'totalDuration','totalGCTime','totalInputBytes','totalShuffleRead','totalShuffleWrite','isBlacklisted','maxMemory']
        mparams = ['usedOnHeapStorageMemory','usedOffHeapStorageMemory','totalOnHeapStorageMemory','totalOffHeapStorageMemory']
        rows = []

        for exe in self.sparkdoc.findall("allexecutors/list/dict"):
            row = []
            for param in params:
                row.append( tydoc.scalenum(exe.find(param).text, minscale=1_000_000) )
            for mparam in mparams:
                row.append( tydoc.scalenum(exe.find(f"memoryMetrics/dict/{mparam}").text))
            rows.append( row )

        def driverkey(x):
            """Handle some of the keys being unsortable: e.g. 'driver' """
            try:
                return int(x[0])
            except Exception:
                return -1000
            
        rows.sort(key=driverkey)
        self.memorymetrics=rows
        
        t.set_caption("Spark executors")
        t.add_head(params+mparams)
        for row in rows:
            t.add_data(row)

        t = doc.table()
        t.set_option(tydoc.OPTION_LONGTABLE)
        t.set_caption("Storage RDDs")
        params = ['id','name','numPartitions','numCachedPartitions','storageLevel','memoryUsed','diskUsed']
        rows = []
        for rdd in self.sparkdoc.findall("storage_rdd/list/dict"):
            row = []
            for param in params:
                row.append( tydoc.safenum(rdd.find(param).text) )
            rows.append(row)
        rows.sort(key=driverkey)
        t.add_head(params)
        for row in rows:
            t.add_data(row)

if __name__=="__main__":
    from argparse import ArgumentParser,ArgumentDefaultsHelpFormatter
    parser = ArgumentParser( formatter_class = ArgumentDefaultsHelpFormatter,
                             description="Print basic information about a DFXML file. Optionally produce more." )
    parser.add_argument("--prettyprint", action='store_true')
    parser.add_argument("file", nargs="?", help="decode this file")
    parser.add_argument("--sparkinfo", help="Just print the spark report to stdout; for testing", action="store_true")
    args  = parser.parse_args()

    if args.file.endswith(".log"):
        dl = DASLogfile(daslog=args.file)
    elif args.file.endswith(".dfxml"):
        dl = DASLogfile(dfxml=args.file)
    else:
        raise RuntimeError("Cannot determine file type of: {}".format(args.file))

    if args.prettyprint:
        dl.prettyprint()
        exit(0)

    if args.sparkinfo:
        print(dl.sparkReport())
        exit(0)

    print("DAS date range: {} to {}".format(dl.first_date,dl.last_date))
    print("STATS FILE: ",dl.gurobi_stats_file())

