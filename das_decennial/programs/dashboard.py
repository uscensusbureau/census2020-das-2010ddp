"""
a collection of tools for interfacing with the dashboard.

"""

# These are hard coded for now. Sorry!

DASEXPERIMENTAL_URL="https://dasexperimental.ite.ti.census.gov"
RANDOM_MISSION_URL = DASEXPERIMENTAL_URL + "/cgi-bin/random_mission.cgi"
DAS_LOG_URL = DASEXPERIMENTAL_URL + "/cgi-bin/daslog.cgi"
DAS_DEBUG_LOG_URL = DASEXPERIMENTAL_URL + "/~$JBID/cgi-bin/daslog.cgi"

CODE_OK = 0

import os
import sys
import time
import urllib.error
import json
import logging
from urllib.parse import urlencode

try:
    import constants as C
except ImportError as e:
    sys.path.append( os.path.join( os.path.dirname(__file__),".."))
    sys.path.append( os.path.join( os.path.dirname(__file__),"../das_framework"))
    import constants as C

from constants import CC
import das_framework.ctools.aws as aws

START_TIME = time.time()
DEBUG=False


def get_mission_name():
    """Return a mission name and put it in the global environment. If it is already in the global environment, use that mission name"""
    if CC.MISSION_NAME not in os.environ:
        try:
            mission_name = aws.get_url(RANDOM_MISSION_URL, ignore_cert=True).strip().upper().replace(" ","_")
            set_mission_name(mission_name)
        except urllib.error.HTTPError:
            return '(bad mission URL)'
        except urllib.error.URLError:
            return 'UNKNOWN MISSION -- dasexperimental is down'
    return os.environ[CC.MISSION_NAME]

def set_mission_name(mission_name):
    os.environ[CC.MISSION_NAME] = mission_name


def make_int(s):
    if s.lower().endswith('g'):
        return int(s[0:-1]) * 1024*1024*1024
    elif s.lower().endswith('m'):
        return int(s[0:-1]) * 1024*1024
    elif s.lower().endswith('k'):
        return int(s[0:-1]) * 1024
    else:
        return int(s)


def das_log(message=None, verbose=False, debug=False, debugurl=False, log_spark=False, extra={}):
    """Send a message to the dassexperimental server using REST API. 
    Each DAS RUN has a run_id. This needs to be provided. If the global variable hasn't been set, 
    then we assume that this is a new run and we get the newrun parameter. Other parameters are added if we can learn them.

    if message is not None, that message is appended to das_log.
    If message is none, then only das_runs table is updated.

    @param message - if not None, then this message is appended to the das_log table
    @param verbose - display message on stdout
    @param debug   - prints details of RPC operation
    @param debugurl - Send REST messages to debug server. URL is defined above in global variables.
    @param log_spark - Log spark-specific information in das_runs. This only needs to be done once.
    extra - additional key/value pairs to send to CGI server. They may be recorded in das_runs.

    """
    if DEBUG:
        verbose=True
        debug=True
    if verbose:
        print(f"DAS_LOG: t={round(time.time() - START_TIME,2)} {message} (sent to DASHBOARD)")

    obj = {**{'clusterId':os.environ[CC.CLUSTERID_ENV], 
              'instanceId': aws.instanceId()}, **extra}

    try:
        run_id = os.environ[CC.RUN_ID]
        obj[CC.RUN_ID] = run_id
    except KeyError:
        if verbose:
            print("DAS_LOG: NEW RUN!!!")
        obj['newrun'] = '1'

    if message:
        obj['message'] = message

    if 'JBID' in os.environ:
        obj['jbid'] = os.environ['JBID']

    if log_spark:
        from pyspark.sql import SparkSession
        conf = SparkSession.builder.getOrCreate().sparkContext.getConf()
        def getSparkOption(conf, spark_option):
            option = conf.get(spark_option)
            if option is None:
                return None
            return make_int(option)
        obj['driver_memory']   = getSparkOption(conf, 'spark.driver.memory')
        obj['executor_memory'] = getSparkOption(conf, 'spark.executor.memory')
        obj['executor_cores']  = getSparkOption(conf, 'spark.executor.cores')
        obj['num_executors']   = getSparkOption(conf, 'spark.executor.instances')
        obj['max_result_size'] = getSparkOption(conf, 'spark.driver.maxResultSize')

    if CC.MISSION_NAME in os.environ:
        obj['mission_name'] = os.environ[CC.MISSION_NAME]

    import gurobipy as gb
    obj['gurobi_version'] = ".".join((map(str,gb.gurobi.version())))
    obj['python_executable'] = sys.executable

    if debugurl:
        url = os.path.expandvars(DAS_DEBUG_LOG_URL)
    else:
        url = DAS_LOG_URL
    url += "?" + urlencode(obj)
    if debug:
        print("das_log: obj=",obj)
        print("das_log: url=",url)
    try:
        ret = aws.get_url(url, ignore_cert=True)
        if debug:
            print("das_log: ret=",ret)

        try:
            ret = json.loads(ret)
        except (TypeError,json.decoder.JSONDecodeError) as e:
            logging.warning(e)
            return
        try:
            os.environ[CC.RUN_ID] = ret[CC.RUN_ID]
        except KeyError:
            pass
        return ret
    except urllib.error.HTTPError as e:
        print("error:",e)
        pass
    except urllib.error.URLError as e:
        print("error:",e)
        pass
    return None
    
def heartbeat():
    """Tell the server we are still alive if we have a RUN_ID. This may be
run in another thread. No locking is required on os.enviorn[] because
of the GIL"""
    if CC.RUN_ID in os.environ:
        das_log(extra={'heartbeat':1})
        
if __name__=="__main__":
    from argparse import ArgumentParser,ArgumentDefaultsHelpFormatter
    parser = ArgumentParser( formatter_class = ArgumentDefaultsHelpFormatter,
                             description="Test the dashboard." )
    parser.add_argument("--mission",help="get a mission and print it",action='store_true')
    parser.add_argument("--debug", action='store_true')
    parser.add_argument("--debugurl", action='store_true')
    parser.add_argument("--log",help="save a log message",nargs='+')
    parser.add_argument("--runtest", action='store_true', help="start a new run and print the runID")
    args = parser.parse_args()
    if args.mission:
        print(get_mission_name())
    if args.log:
        das_log(" ".join(args.log), debug=args.debug, debugurl=args.debugurl)
    if args.runtest:
        das_log("this is the start of a test run", debug=args.debug, debugurl=args.debugurl)
        das_log("this is the end of a test run", debug=args.debug, debugurl=args.debugurl, extra={'stop':'now()'})
    
