#!/usr/bin/env python3
# -*- mode: python; -*-
"""das_performance/das_stats.py provides tools for accessing the statistics that Amazon automatically creates for EMR nodes.
It can also interface with matplotlib.
Unfortunately, much of the data that Amazon writes out is unformatted, so it must be parsed.

The class populates a data model of a Cluster class and a AWSNode
class. Each node has a list of Snapshot classes, which are
dictionaries.

Note that Amazon's documentation uses the terms 'instance' and 'node' interchangably. 

"""

from collections import defaultdict
import datetime
import dateutil.parser
import gzip
import itertools
import json
import multiprocessing
import os
import os.path
import re
import subprocess
import sys
import time
import zipfile
import glob
import logging

# This is to support standalone operation
try:
    import das_framework
    import das_performance
except ImportError:
    sys.path.append( os.path.dirname(__file__))
    sys.path.append( os.path.join(os.path.dirname(__file__), ".."))

import das_performance.syslog_gurobistats  as syslog_gurobistats 



import das_framework.ctools.s3 as s3
import das_framework.ctools.cspark as cspark
import das_framework.ctools.aws as aws
import das_framework.ctools.emr as emr
import das_framework.ctools.tydoc as tydoc

from das_framework.ctools.timer import Timer
from das_framework.ctools.s3    import _Key,_Size
from das_framework.ctools.emr   import clusterId
from das_framework.ctools       import latex_tools

import das_performance.graph
from das_performance.gurobi_stats import GurobiStats
from das_performance.daslog import DASLogfile

import constants as C
from constants import CC

# DEFAULT_THREADS=100
MAX_THREADS=300
DEFAULT_THREADS = min(MAX_THREADS,os.cpu_count()*10)
EMR_LOG_BUCKET_NAME=os.environ[CC.DAS_S3ROOT_ENV].replace("s3://","")+"-logs"

# j-1VYQWTUI84GS2/node/i-069048ddc42e48d6c/daemons/instance-state/instance-state.log-2018-10-26-15-15.gz 

VM_COLLECT_LAUNCH='/mnt/gits/das-vm-config/bootstrap/vm_collect_launch'
VM_COLLECTION_INTERVAL_RUNNING=15          # measure speed and memory every 15 seconds while DAS is running
VM_COLLECTION_INTERVAL_DEFAULT=60*5        # measure speed and memory every 5 minutes when done


## Fields that may be put in each node statistic
DATETIME = 'time'               # When the stat was created
LOAD_AVERAGE = 'load_average'   # value is the 3 averages
CPU_STATS = 'cpu_stats'
MEM_STATS = 'mem_stats'
ALERTS = 'alerts'               # obj[ALERTS][level] is a list
MESSAGES = 'messages'           # obj[MESSAGES][] is a list of tuples (time,message)
PROCESSES = 'processes'
EMERG = 'EMERG'
ALERT = 'ALERT'
CRIT = 'CRIT' 
ERR  = 'ERR'
WARNING='WARNING'
NOTICE='NOTICE'
CORES = 'CORES'
UPTIME_SECONDS = 'uptime_seconds'

FIRST_DATE = "2018-01-01"
END_DATE=(datetime.datetime.now() + datetime.timedelta(days=1)).isoformat()

IGNORE_MESSAGES = ['serial8250: too much work',
                   'error 4 in sshd',
                   'hrtimer: interrupt took',
                   'Found a Tx']


def is_ignorable(msg):
    for ign in IGNORE_MESSAGES:
        if ign in msg:
            return True
    return False

def set_collection_interval(interval):
    if os.path.exists(VM_COLLECT_LAUNCH):
        subprocess.check_call([VM_COLLECT_LAUNCH,'--quiet','--interval',str(interval)])

def upload(fname,s3fname=None):
    """Upload a file to our S3 bucket"""
    if not s3fname:
        s3fname = C.DAS_S3_LOGFILE_TEMPLATE.format(DAS_S3ROOT=os.getenv(CC.DAS_S3ROOT_ENV),
                                                   CLUSTERID=os.getenv(CC.CLUSTERID_ENV), 
                                                   fname=os.path.basename(fname))
        print("s3fname:",s3fname)
    
    (bucket,key) = s3.get_bucket_key(s3fname)
    s3.put_object(bucket, key, fname)
    print(f"upload: aws s3 cp {fname} s3://{bucket}/{key}")
    logging.info(f"upload: {fname} -> s3://{bucket}/{key}")


################################################################
### 
### Data Extraction Tools
###

def grep(lines,text):
    """Return the line numbers of the lines that contain text"""
    return [number for (number,line) in enumerate(lines) if text in line]

class SnipError(RuntimeError):
    pass

def snip(lines,start_text,end_text,start_count=0,end_count=0,start_offset=0,end_offset=0):
    """Return the lines are between start_text and end_text (do not include end_text).
    If more than one line has the text, generate an error"""
    l0 = grep(lines,start_text)
    if not l0:
        raise SnipError("start_text '{}' not found".format(start_text))

    l1 = grep(lines,end_text)
    if not l1:
        raise SnipError("end_text '{}' not found".format(end_text))

    try:
        return lines[l0[start_count]+start_offset:l1[end_count]+end_offset]
    except IndexError as i:
        raise SnipError("IndexError.  len(lines)={}  l0={}  l1={}  start_count={}  end_count={}".
                        format(len(lines),l0,l1,start_count,end_count))
    

# For the PS_HEADER don't grab the fields that are variable width
#
PS_HEADER="PID %CPU %MEM"
load_average_re=re.compile("load average: ([0-9.]+), ([0-9.]+), ([0-9.]+)")
cpunum_re = re.compile("cpuhp/(\d+)")
uptime_re = re.compile("(\d+):(\d+):(\d+) up (\d+) days?,\s+(\d+):(\d+)")
kernelmsg_re = re.compile(r"\[([\d.]+)\] ([a-zA-Z].*)")

import xml.etree.ElementTree as ET
def get_text(tree,tag):
    try:
        return tree.find(tag).text
    except AttributeError as e:
        return ""

def get_int(tree,tag):
    return int(get_text(tree,tag))

MiB = 1024*1024
def extract_dfxml_state(data,debug=False):
    """Given the DFXML file, create a dictionary of the node's snapshot at a given time."""
    ret = []
    if debug:
        print("Processing multi-line DFXML object {} bytes".format(len(data)))
    for count,line in enumerate(data.split("\n"),1):
        obj = {}
        try:
            tree = ET.fromstring(line)
        except ET.ParseError as e:
            if debug:
                print("Cannot parse:",line)
            continue
        try:
            start_time    = get_text(tree,".//start_time")
            obj[DATETIME] = dateutil.parser.parse(start_time)
            loadavg_attrib = tree.find(".//loadavg").attrib
            obj[LOAD_AVERAGE] = [float(loadavg_attrib[k]) for k in ['avg1','avg5','avg15']]
            obj[MEM_STATS]= { 'total': get_int(tree, ".//virtual_memory/total") / MiB,
                              'used':  get_int(tree, ".//virtual_memory/used")  / MiB,
                              'free':  get_int(tree, ".//virtual_memory/free")  / MiB}
            cpuinfo       = json.loads(get_text(tree,".//cpuinfo"))
            obj[CORES]    = len(cpuinfo)
            
            ret.append(obj)
        except (SnipError,IndexError,AttributeError) as e:
            if debug:
                print("DEBUG: {}".format(e))
            pass

    if debug:
        print("DFXML extraction of {} byte object produced {} objects".format(len(data),len(ret)))

    return ret


################################################################
###
### Data Model

# Information for the node
class AWSNode:
    """Represents information about a specific node (VM)"""

    @classmethod
    def dont_print(*args, **kwargs):
        pass

    def __init__(self, instanceID=None, instanceInfo=None, debug=False, verbose=False, cache=True):
        self.instanceID   = instanceID
        self.stats        = []     # list of dictionaries
        self.instanceInfo = instanceInfo
        self.first_message = True
        self.debug         = debug
        self.cache         = cache
        if verbose:
            self.vprint   = print
        else:
            self.vprint   = self.dont_print

    def extract_instance_state(self, s3url, data):
        """Given the text file from Amazon, create a dictionary of the node's
        snapshot at a given time. Returns as a list for compatiability with
        the DFXML version of this."""
        # Given an instance state file as an input, extract useful information.
        obj = {}
        lines = data.split("\n")
        if len(lines)<50:           # too short!
            return []

        obj[DATETIME] = datetime.datetime.strptime(lines[2][4:],"%b %d %H:%M:%S %Z %Y")
        uptime         = snip(lines,"uptime","# whats running",start_offset=+1,end_offset=-1)
        obj[PROCESSES] = snip(lines,PS_HEADER,"# Top CPU users",start_offset=1,end_offset=-1)

        # Figure out when we boot
        m = uptime_re.search(uptime[0])
        if m:
            obj[UPTIME_SECONDS] = int(m.group(4))*24*60*60 + int(m.group(5))*60 + int(m.group(6))


        # Find how many cores on this system
        max_cpu = 0
        for line in obj[PROCESSES]:
            m = cpunum_re.search(line)
            if m:
                cpunum = int(m.group(1)) +1
                if cpunum>max_cpu:
                    max_cpu = cpunum

        obj[CORES] = max_cpu

        m = load_average_re.search(uptime[0])
        if m:
            obj[LOAD_AVERAGE] = [float(val) for val in m.group(1,2,3)]

        try:
            cpu  = snip(lines,'iostat -x 1 5','iostat -x 1 5',start_offset=4,end_offset=5)
            cpu_stats      = dict(zip(['user','nice','system','iowait','steal','idle'],
                                  [float(val) for val in cpu[0].split()]))
            obj[CPU_STATS] = cpu_stats
        except SnipError as e:
            if self.debug:
                print("DEBUG1: {}".format(e))

        try:
            mem  = snip(lines,'# whats memory usage look like','# trend memory')
            obj[MEM_STATS] = dict(zip(mem[2].strip().split(),
                                      [float(val) for val in mem[3].strip().split()[1:]]))
        except SnipError as e:
            if self.debug:
                print("DEBUG2: {}".format(e))

        try:
            disk = snip(lines,'# amount of disk free','# amount of disk free',start_offset=0,end_offset=10)
        except SnipError as e:
            if self.debug:
                print("DEBUG3: {}".format(e))

        # Get the alerts
        obj[ALERTS] = {}
        for level in [EMERG,ALERT,CRIT,ERR,WARNING,NOTICE]:
            obj[ALERTS][level] = [lines[m] for m in grep(lines,level)]

        # Look for kernel errors (e.g. syslog)
        obj[MESSAGES] = []
        if UPTIME_SECONDS in obj:
            l0 = grep(lines,'hows the kernel looking')
            l1 = grep(lines,'dump instance controller log')
            if l0 and l1:
                for line in lines[l0[0] : l1[0]]:
                    m = kernelmsg_re.search(line)
                    if m:
                        when = obj[DATETIME] - datetime.timedelta(seconds=obj[UPTIME_SECONDS]) + datetime.timedelta(seconds=float(m.group(1)))
                        what = m.group(2)
                        if not is_ignorable(what):
                            if self.first_message:
                                self.first_message = False
                                if self.debug:
                                    print(f"kernel messages from {s3url}")
                            if self.debug:
                                print(f"{when} {what}")
                            obj[MESSAGES].append((when,what))
        return [obj]

    def stats_for_s3url(self,s3url):
        """Given an S3 URL in the form s3://bucket/key, get the data, decompress it, parse, 
        and return a list of objects. This is a slow process,
        and we use multithreading to make it run quickly."""

        if self.debug:
            print("Download {}".format(s3url))

        download_data = s3.s3open(s3url,'rb',cache=self.cache).read()
        if s3url.endswith(".gz"):
            data = gzip.decompress(download_data).decode('utf-8')
        else:
            data = download_data.decode('utf-8')

        if data.startswith("<dfxml>"):
            return extract_dfxml_state(data,debug=self.debug)
        try:
            return self.extract_instance_state(s3url,data)
        except SnipError as e:
            self.vprint("Snip error processing {}:\n{}\n".format(s3url,e))
            return []

    def download_and_parse_s3keys(self,*,bucket,s3keys,threads=1,limit=None):
        # Make sure we have keys
        if not s3keys:
            return              

        # Find the keys we are going to download
        download_keys = [f's3://{bucket}/{key}' for key in s3keys]
        download_keys.sort()
        if limit:
            download_keys = download_keys[0:limit]

        # Get and parse the files, filter out the bad data, and store it all in the stats attribute
        # of the node.
        threads = min(threads, len(download_keys))
        self.vprint("{}: Using {} threads to access {:,} URLs".format(self.instanceID,threads,len(download_keys)))
        with multiprocessing.Pool(threads) as p:
            stat_lists = p.map(self.stats_for_s3url, download_keys)
            self.stats = list(itertools.chain.from_iterable(stat_lists))
            
        self.vprint("{}: Downloaded {:,} statistics".format(self.instanceID,len(self.stats)))


class Cluster:
    """Represents information for a group of nodes that have a specific clusterId.

    The list of nodes in the cluster can be obtained with the EMRAPI (which is subject to rate limiting)
    or from S3 (which requires that the EMR_LOG_BUCKET_NAME be correct.)

    """
    def __init__(self,clusterId):
        self.clusterId  = clusterId
        self.nodes      = defaultdict(AWSNode) # dictionary by instanceID

    def get_info_from_aws(self, useEMRAPI=False):
        """Load cluster stats from Amazon API"""
        print("get_info_from_aws for cluster",self.clusterId,"useEMRAPI=",useEMRAPI)
        if useEMRAPI:
            with aws.Proxy() as p:
                self.describe_cluster = emr.describe_cluster(self.clusterId)
                instances = emr.list_instance(self.clusterId)
            for instanceInfo in instances:
                Ec2InstanceId = instanceInfo['Ec2InstanceId']
                self.nodes[Ec2InstanceId] = AWSNode(Ec2InstanceId,instanceInfo=instanceInfo)

        else:
            self.describe_cluster = None # will not get this information
            for line in subprocess.check_output(['aws','s3','ls',os.path.join(EMR_LOG_BUCKET_NAME,self.clusterId,'node')+'/'],encoding='utf-8').split("\n"):
                line = line.strip()
                if line.startswith('PRE'):
                    Ec2InstanceId = line.split()[1].replace("/","")
                    self.nodes[Ec2InstanceId] = AWSNode(Ec2InstanceId,instanceInfo=None)


    def print_info(self):
        assert len(self.nodes)>0
        print(f"Cluster {self.clusterId} has had a total of {len(self.nodes)} nodes over its lifetime.")
    
    def instance_type(self,Ec2InstanceId):
        """Given an InstanceID, return the InstanceType"""
        try:
            return self.nodes[Ec2InstanceId].instanceInfo['InstanceType']
        except AttributeError as e:
            return "n/a"

def log_prefix_with_date(prefix,dt):
    """Give a log prefix with a DATETIME, replace with time in AWS format."""
    sub = f"{dt.year}-{dt.month:02}-{dt.day:02}-{dt.hour:02}-{dt.minute:02}-{dt.second:02}"
    return prefix.replace("DATETIME",sub)

def get_s3keys_for_node(node,debug=False):
    if debug:
        print("Getting s3keys for node {}".format(node.instanceID))
    s3keys = []
    for (node_prefix,log_re) in [ (f'{node.prefix}/{node.instanceID}/DAS/dfxml-instance-state/dfxml.log.DATETIME.dfxml',
                                   DFXML_STATE_RE) ,
                                  (f'{node.prefix}/{node.instanceID}/daemons/instance-state/instance-state.log-DATETIME.gz',
                                   INSTANCE_STATE_RE)]:

        # Only list files that might be in our time of interest
        prefix1 = log_prefix_with_date(node_prefix, node.start)
        prefix2 = log_prefix_with_date(node_prefix, node.end)
        cprefix  = os.path.commonprefix([prefix1,prefix2])

        # Very strange - when we print os.environ, we do not get an error.
        # When we don't print it, we do. And this has something to do with multiprocessing.
        # So how about if we just use it?

        for obj in s3.list_objects(node.bucket,cprefix,limit=node.limit):
            key = obj[_Key]
            m = log_re.search(key)
            if m:
                instanceID2 = m['node']
                assert node.instanceID == instanceID2
                s3keys.append(key)
    return (node,s3keys)

def get_data_for_node(node,s3keys):
    # Now get the data from S3 using the multi-threaded downloader
    node.download_and_parse_s3keys(bucket=node.bucket, s3keys=s3keys,threads=node.threads, limit=node.limit)
    print(f"Retrieved {len(node.stats)} stats for {node.instanceID}",end='')
    if node.stats:
        t0 = min([obj[DATETIME] for obj in node.stats])
        t1 = max([obj[DATETIME] for obj in node.stats])
        print(f" from {t0} to {t1}")
    else:
        print("")

# Recast this function as method
def get_cluster_stats(clusterId,limit=None,threads=DEFAULT_THREADS,start=dateutil.parser.parse(FIRST_DATE),end=dateutil.parser.parse(END_DATE)):
    """Gets the instance-state files and the DFXML files between start and end"""

    print(f"get_cluster_stats({clusterId},{limit},{threads},{start},{end})")
    start = datetime.datetime(start.year,start.month,start.day,start.hour,start.minute,0)
    end   = datetime.datetime(end.year,end.month,end.day,end.hour,0,0) + datetime.timedelta(hours=1)

    (bucket,prefix) = bucket_prefix_for_cluster(clusterId)

    print(f"Generating report for clusterId {clusterId} from {start} to {end}")
    ci = Cluster(clusterId=clusterId)
    ci.get_info_from_aws()
    ci.print_info()


    #
    # Set variables that will be used by the node to pull data
    #
    for node in ci.nodes.values():
        node.bucket = bucket
        node.prefix = prefix
        node.start  = start
        node.end    = end
        node.limit  = limit
        node.threads = threads  # 

    return ci
    # Below is generating errors...

    # Give each node the bucket and the prefix. The node objects get passed

    # In parallel, get the s3 keys for each node
    with multiprocessing.Pool(threads) as p:
        nodes_and_keys = p.map(get_s3keys_for_node, ci.nodes.values())

    # Now in parallel, download the keys for each node
    for (node,s3keys) in nodes_and_keys:
        if s3keys:
            get_data_for_node(node,s3keys)
    return ci



################################################################
# Find the S3 files to download
#
# regular expression to find instance state logfile

def bucket_prefix_for_cluster(cluster):
    """Returns the bucket and the prefix for a cluster"""
    return s3.get_bucket_key('s3://' + EMR_LOG_BUCKET_NAME + '/' + cluster + '/node')

# regular expression to find the AWS files associated with instance state
INSTANCE_STATE_RE = re.compile('(?P<cluster>j-[^/]+)/node/(?P<node>i-[^/]+)/daemons/instance-state/'+
                               'instance-state.log-(?P<date>.*).gz')

# regular expression to find the DFXML files associated with instance state
DFXML_STATE_RE = re.compile('(?P<cluster>j-[^/]+)/node/(?P<node>i-[^/]+)/DAS/dfxml-instance-state/'+
                               'dfxml.log.(?P<date>.*).dfxml(.gz)?')

# regular expression to pull date out of a filename
LOGFILENAME_DATE_RE = re.compile(r"[^\d](\d\d\d\d)-(\d\d)-(\d\d)-(\d\d)-(\d\d)[^\d]")

def date_from_log_filename(filename):
    """Pull the date out a filename; return as a datetime object."""
    m = LOGFILENAME_DATE_RE.search(filename)
    if not m:
        raise ValueError("No date in: {}".format(filename))
    return datetime.datetime(int(m.group(1)),int(m.group(2)),int(m.group(3)),
                             int(m.group(4)),int(m.group(5)),0)

def time_range_for_node(cluster,node):
    prefix = cluster + '/node/' + node + '/daemons/instance-state/'
    times = [obj['Key'].replace(prefix,'') for obj in s3.list_objects(EMR_LOG_BUCKET_NAME, prefix, delimiter='/')]
    if not times:
        return (None, None)
    t0 = times[0].replace('instance-state.log-','').replace('.gz','')
    t1 = times[-1].replace('instance-state.log-','').replace('.gz','')
    t0 = t0[0:10] + " " + t0[11:13] + ":" + t0[14:16]
    t1 = t1[0:10] + " " + t1[11:13] + ":" + t1[14:16]
    return (t0,t1)

def nodes_for_cluster(cluster):
    prefix = cluster + '/node/'
    ret = [obj['Prefix'].replace(prefix,'').replace('/','') for obj in s3.list_objects(EMR_LOG_BUCKET_NAME, prefix, delimiter='/')]
    return ret

def compile_stats(dl, threads=DEFAULT_THREADS, addSyslogStats=False, stat_dict_list=None, **opts):
    """
    @param dl - daslog object.
    @param - threads - how many threads to use to access S3. Legacy and probably going away due to overhead.
    @param - addSyslogStats - grab the stats from syslog
    @param - stat_dict_list - a list of gurobi stats dictionary objects.
    """
    with Timer(message="Time to collect stats from S3: %.03f") as t:
        print("Getting CPU and memory statistics from S3")
        ci = get_cluster_stats(dl.clusterId(),threads=threads,**opts)
        print("CPU and memory statistics acquired and parsed.")

    # Build the document for the stats report
    doc = tydoc.tydoc()

    #If spark is running, use Spark to get basic gurobi stats
    #if cspark.spark_running():
    #    stats_file = dl.gurobi_stats_file()
    #    if stats_file:
    #        gs   = GurobiStats( stats_file, start=dl.first_date, end=dl.last_date )
    #        gs.add_gurobi_report(doc)
    #    else:
    #        doc.p("HTML report created with Spark running, but no Gurobi statistics file was found")
    #else:
    #    doc.p("HTML report created without Spark running, so no gurobi statistics are included in this file.")

    # Build the S3-Gurobi performance graph and add it to the document
    graph = das_performance.graph.EMR_Graph(ci=ci, dl=dl)
    graph.insert_graph_data(doc,describe_cluster=True)
    save_base=os.path.splitext(dl.log_filename)[0]
    # Real-tie gurobi stats from syslog. 
    if stat_dict_list is not None:
        gsl = syslog_gurobistats.GurobiSyslog(doc=doc)
        gsl.createDataFrame(stat_dict_list)
        gsl.run()
        gsl.save(save_base)
    elif addSyslogStats:
        # Build the syslog local1.log performance graph and add it to the document
        if dl.appId:
            syslog_gurobistats.compile_and_insert_graph_data(doc=doc,dl=dl, save_base=save_base)
        else:
            logging.warning("appId not set; will not generate syslog gurobi statistics ")
    else:
        logging.info("syslog stats not added")

    dl.addTimestamps(doc)
    dl.addSparkExecutionReport(doc)    # Add the spark execution report

    # Write out HTML
    html_file = dl.log_filename.replace(".log",".html")
    doc.save( html_file )
    return html_file

def make_zipfile(dfxml_filename, extra=""):
    """
    Make the zip file with everything from the basefilename
    This will include the logfile and the source code release, if present.
    """

    basename = os.path.splitext(dfxml_filename)[0] 

    zipfilename = basename + extra + '.zip'
    if os.path.exists(zipfilename):
        os.unlink(zipfilename)
    filenames = glob.glob( basename+'.*' )
    with zipfile.ZipFile(zipfilename, 'w', zipfile.ZIP_DEFLATED) as zf:
        for filename in filenames:
            zf.write( filename, os.path.basename(filename))
            print("Added {} to zipfile {}".format(filename, zipfilename))

    return zipfilename
       

def run_stats_for_logfile(*,daslog=None,dfxml=None,threads=DEFAULT_THREADS, extra="", config=None, stat_dict_list=None):
    """Called from das2020_driver.py in a production run, and optionally called from __main__ below during testing and development.
    This function reads the daslog (either as a .log file to find the .dfxml file, or as the .dfxml file directly) and uses
    that to find the location in the S3 bucket of the Gurobi stats. 
    After the Gurobi stats are processed from S3, it then reads the syslog stats from /var/log/local1.log.
    The syslog log messages are only available on the MASTER node of where the DAS was originally run.
    (The other analysis step can be done on *any* node.)
    """

    ### First we read the data
    print(f"run_stats_for_logfile(daslog={daslog},dfxml={dfxml})")
    dl = None
    opts = {}
    with Timer(message="Reading stats total time: %.03f seconds") as t:
        if daslog:
            dl = DASLogfile(daslog=daslog)
        elif dfxml:
            dl = DASLogfile(dfxml=dfxml)
        else:
            dl = None
        if dl:
            opts['start'] = dateutil.parser.parse(dl.first_date.isoformat())
            opts['end']   = dateutil.parser.parse(dl.last_date.isoformat())

            print("DAS logfile runs from {} to {}".format(opts['start'],opts['end']))
            
    ### Optionally generate a .html file with the Gurobi statistics
    #if config.getboolean(C.GUROBI_SECTION, C.SAVE_STATS_OPTION, fallback=False):
    compile_stats(dl, threads=threads, stat_dict_list=stat_dict_list, **opts)
    
    ### Now we make a ZIP file that is stored in S3 and picked up by the dashboard server
    # this should have created both report.pdf and report.zip
    # Upload them to our monitoring system
    zipfilename = make_zipfile(dl.dfxml_filename, extra=extra)

    if config:
        stats_dir  = os.path.expandvars( config[C.WRITER].get(C.STATS_DIR, C.STATS_DIR_DEFAULT) )
    else:
        stats_dir = os.path.expandvars( C.STATS_DIR_DEFAULT )

    ### Finally upload to S3.
    upload( fname = zipfilename, s3fname = os.path.join(stats_dir,zipfilename))

   
def list_available_clusters():
    print("Available clusters:")
    for obj in s3.list_objects(EMR_LOG_BUCKET_NAME,'',delimiter='/'):
        cluster = obj['Prefix'].replace('/','')
        print(f'Cluster: {cluster:16}  ',end='', flush=True)
        nodes = nodes_for_cluster(cluster)
        print(f'nodes: {len(nodes):2}  ',end='', flush=True)
        (t0,t1) = time_range_for_node(cluster,nodes[0])
        print(f'from {t0} to {t1}')
    
if __name__=="__main__":
    from argparse import ArgumentParser,ArgumentDefaultsHelpFormatter
    clusterId = emr.clusterId()

    parser = ArgumentParser( formatter_class = ArgumentDefaultsHelpFormatter,
                             description="Generate and graph stats on current cluster" )
    parser.add_argument("--list",    help="List available clusters", action='store_true')
    parser.add_argument("--cluster", help="specify nodes in this cluster", default=clusterId)
    parser.add_argument("--start",   help="Specify start time for stats in ISO8601 format (e.g. 2018-10-10T10:10)")
    parser.add_argument("--end",     help="specify end time for stats in ISO8601 format")
    parser.add_argument("--parse",   help="Just parse the specified node stats file (for testing)")
    parser.add_argument('--verbose', help="print parsing errors", action='store_true')
    parser.add_argument('-j',"--threads", type=int, default=DEFAULT_THREADS, help="Use this many threads by default")
    parser.add_argument("--debug",   help='print downloads', action='store_true')
    parser.add_argument("logfile", nargs='?', help='Either a logfile or a dfxml file')

    args   = parser.parse_args()

    if cspark.spark_running():
        cspark.spark_set_logLevel('ERROR')


    if args.parse:
        datalist = extract_instance_state(open(args.parse,"r").read())
        for data in datalist:
            print("data:")
            for key,val in data.items():
                print(key,str(val)[0:100])
        exit(0)
    
    if args.list:
        list_available_clusters()
        exit(0)

    if args.logfile:
        if args.logfile.endswith(".dfxml"):
            run_stats_for_logfile(dfxml=args.logfile,threads=args.threads)
        elif args.logfile.endswith(".log"):
            run_stats_for_logfile(daslog=args.logfile,threads=args.threads)
        
        print("")
        print("DAS run stats completed.")
        print("")
