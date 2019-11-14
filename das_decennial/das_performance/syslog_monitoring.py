#!/usr/bin/evn python3
"""Gurobi monitoring from syslog"""

"""
Sample entry:
Aug 11 12:32:27 ip-10-252-44-107 nodes.py: 415 (addGurobiStatistic) {"optimizer": "GeoRound", "instanceId": "i-091a901385f4970e3", "uuid": "213aae1fa5da438f9e665d81d201b5d6", "t": 1565526747.4436197, "t_env_start": null, "t_env_end": null, "t_modbuild_start": null, "t_modbuild_end": null, "t_presolve_start": null, "t_presolve_end": null, "t_optimize_start": 1565526073.7273395, "t_optimize_end": 1565526747.4393368, "point": 4, "geocode": "44", "parentGeocode": "0", "childGeolevel": "State", "stack": "./das_decennial_submodules.zip/programs/optimization/optimizer.py:438(setObjAndSolve)\u2022./das_decennial_submodules.zip/programs/optimization/geo_optimizers.py:298(run)\u2022./das_decennial_submodules.zip/programs/optimization/sequential_optimizers.py:111(run)", "failsafe_invoked": false, "pid": 80093, "ppid": 17084, "loadavg": 1.02, "utime": 2313.24, "stime": 1191.796, "maxrss_bytes": 1125883904, "utime_children": 0.0, "stime_children": 0.004, "maxrss_children": 145040, "childGeoLen": 1, "NumVars": 467712, "NumConstrs": 22, "NumSOS": 0, "NumQConstrs": 0, "NumGenConstrs": 0, "NumNZs": 1533168, "NumQNZs": 0, "NumIntVars": 467712, "NumBinVars": 467712, "NumPWLObjVars": 0, "MIPGap": 0.0, "Runtime": 673.7119669914246, "Status": 2, "IterCount": 3.0, "BarIterCount": 0, "NodeCount": 0.0, "IsMIP": 1, "IsQP": 0, "IsQCP": 0, "Threads": 64, "TuneCriterion": -1, "model_status": 2, "gurobi_version": "8.1.0", "did_presolve": true, "error": "", "error_message": "", "applicationId": "application_1554293640373_0552"}
"""

# import datetime
import time
# import logging
# import dateutil.parser
# import json
import os
import re 
import sys
# import xml.etree.ElementTree as ET

# import numpy as np
# import pandas as pd
import subprocess

from collections import defaultdict

# This is to support standalone operation
try:
    import das_framework
    import das_performance
except ImportError:
    sys.path.append( os.path.dirname(__file__))
    sys.path.append( os.path.join(os.path.dirname(__file__), ".."))


# import constants as C
from constants import CC
# from das_framework.ctools.timer import Timer
# from das_framework.ctools import tydoc
from das_performance.syslog_gurobistats import extract_syslog_jsondicts_for_appId

# from itertools import groupby

def geocode_to_geolevel_name(geocode):
    try:
        return {1:"National",
                2:"State",
                2+3:"County",
                2+3+4:"Tract_Group",
                2+3+6:"Tract",
                2+3+6+1:"Block_Group",
                2+3+6+1+4:"Block" }[len(geocode)]
    except KeyError:
        return f"Unknown_{len(geocode)}-char_level"


def show_hist(hist):
    return " ".join([f"{key}: {val}" for (key,val) in hist.items()])        

def geolevel_hist(hist):
    counts = defaultdict(int)
    for ec in hist:
        counts[geocode_to_geolevel_name(ec)] += 1
    return show_hist(counts)


class SyslogGurobiStats:
    """maintain the statistics for the syslog monitoring"""
    def __init__(self,update_frequency=1.0):
        self.instance_load = dict()     # load of each node
        self.env_create   = defaultdict(int) # key is geocode
        self.opt_end = defaultdict(int)
        self.update_frequency = update_frequency
        self.last_update = time.time()

    def add(self,obj):
        """Add a statistics object"""
        if 'instanceId' in obj and 'loadavg' in obj:
            self.instance_load[obj['instanceId']] = obj['loadavg']
        if 'point' in obj and obj['point']==CC.POINT_ENV_CREATE:
            self.env_create[obj['geocode']] += 1
        if 'point' in obj and obj['point']==CC.POINT_OPTIMIZE_END:
            self.opt_end[obj['geocode']] += 1
            
    def show(self, force=False):
        """Print the statistics"""
        if (self.last_update + self.update_frequency > time.time()) and not force:
            return
        print("================ STATS ================")
        print(time.asctime())
        print("Node Loads:")
        print(show_hist(self.instance_load))
        print("Created gurobi environments by geolevel:",geolevel_hist(self.env_create))
        print("Completed optimizations by geolevel:",geolevel_hist(self.opt_end))
        print("=======================================")
        print("")
        print("")
        self.last_update = time.time()


def monitor_appId(*,appId,logfile=CC.DEFAULT_LOG_FILE,existing=True,tail=True,stat_dict_list=None,update_frequency=1):
    stats = SyslogGurobiStats(update_frequency=update_frequency)
    for obj in extract_syslog_jsondicts_for_appId(logfile=logfile,appId=appId,existing=existing,tail=tail):
        if stat_dict_list is not None:
            stat_dict_list.append(obj)
        stats.add(obj)
        stats.show()

def get_appId():
    out = subprocess.check_output('yarn application -list -appStates RUNNING'.split(),encoding='utf-8')
    r = re.compile(r"(application_(\S+))")
    m = r.search(out)
    if m:
        return m.group(1)
    raise RuntimeError("Cannot determine appId")
    

if __name__=="__main__":
    from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter,description='Test program to exercise log monitoring system')
    parser.add_argument("--survey", help="Report number of logfile entries for each applicationId",action='store_true')
    parser.add_argument("--appId",help="application Id")
    parser.add_argument("--running", help="Learn from running app")
    args   = parser.parse_args()
    if args.appId:
        pass
    elif args.running:
        raise RuntimeError("--running not implemented")
    elif args.survey:
        monitor_survey()
        exit(0)
    else:
        raise RuntimeError("--appId or --running or --survey must be specified")

    monitor_appId(appId=args.appId,logfile='/var/log/local1.log')
    

