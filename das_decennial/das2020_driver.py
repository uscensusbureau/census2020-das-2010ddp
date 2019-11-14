#!/usr/bin/env python3
"""
das2020_driver is the driver program for the 2020 Disclosure Avoidance System

"""

import json
import logging
import os
import subprocess
import sys
import time
import re
import gc
import threading

if 'SPARK_HOME' not in os.environ:
    os.environ['SPARK_HOME'] = '/usr/lib/spark'

# Add the location of shared libraries
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python'))
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python', 'lib', 'py4j-src.zip'))

# Run the DAS and return the DAS object
from das_performance import das_stats
from das_performance import gc_stats
import das_performance.syslog_monitoring
import constants as C
from constants import CC

import das_framework.ctools as ctools
import das_framework.ctools.clogging as clogging
import das_framework.ctools.cspark as cspark
import das_framework.driver
import programs.das_setup
import programs.dashboard as dashboard

MIN_FRAMEWORK_VERSION='1.0.0'
MIN_CTOOLS_VERSION='1.0.0'


def display_mission(name):
    block_width = 43

    def dl(line, fill=' ', ):
        print('==={}==='.format(line.center(block_width, fill)))

    dl("", fill='=')
    dl("DAS RUN AT {}".format(time.asctime()))
    dl(name)
    dl("", fill='=')

def dashboard_heartbeat(*,config):
    print_heartbeat     = config.getboolean(section=C.GUROBI_SECTION,option=CC.PRINT_HEARTBEAT,
                                            fallback=True)
    heartbeat_frequency = config.getint(section=C.GUROBI_SECTION,option=CC.HEARTBEAT_FREQUENCY,
                                        fallback=CC.HEARTBEAT_FREQUENCY_DEFAULT)
    print_heartbeat_frequency = config.getint(section=C.GUROBI_SECTION,option=CC.PRINT_HEARTBEAT_FREQUENCY,
                                              fallback=CC.HEARTBEAT_FREQUENCY_DEFAULT)
    last_print_time = 0
    while True:
        if print_heartbeat:
            if last_print_time + print_heartbeat_frequency > time.time():
                print("{} pid={} dashboard_heartbeat".format(time.asctime(),os.getpid()))
                last_print_time = time.time()
        dashboard.heartbeat()
        time.sleep( min(heartbeat_frequency, print_heartbeat_frequency ))

if __name__=="__main__":
    """Driver program to run the DAS and then upload the statistics"""

    if das_framework.__version__ < MIN_FRAMEWORK_VERSION:
        raise RuntimeError("das_framework is out of date; please update to version {}".format(MIN_FRAMEWORK_VERSION))
    if (not hasattr(ctools,'__version__')) or ctools.__version__ < MIN_CTOOLS_VERSION:
        raise RuntimeError("das_framework.ctools is out of date; please update to version {}".format(MIN_FRAMEWORK_VERSION))


    ## OMG. Make the py4j logger not horrible
    ## See https://stackoverflow.com/questions/34248908/how-to-prevent-logging-of-pyspark-answer-received-and-command-to-send-messag
    logging.getLogger("py4j").setLevel(logging.ERROR)

    # these seem to be not the problem:
    # logging.getLogger('pyspark').setLevel(logging.ERROR)
    # logging.getLogger("matplotlib").setLevel(logging.ERROR)

    # Option processing goes first
    # If we make a BOM, it happens here, and we never return
    (args, config) = das_framework.driver.main_setup()

    ###
    ### Validate Configuration File
    ###
    # Look for deprecated variables
    DEPRECATED_CONFIG_VARIABLES = [(C.OUTPUT_FNAME, C.WRITER)]
    for (var, section) in DEPRECATED_CONFIG_VARIABLES:
        if var in config[section]:
            raise RuntimeError(f"config file contains deprecated variable {var} in section [{section}]")

    ###
    ### Set up the environment as necessary
    ###

    applicationId = clogging.applicationId()
    logging.info("applicationId: %s",applicationId)
    os.environ[CC.APPLICATIONID_ENV] = applicationId
    os.environ[CC.PYTHON_VERSION] = f'python{sys.version_info.major}.{sys.version_info.minor}'

    if CC.CLUSTERID_ENV not in os.environ:
        logging.warning("{} environment variable not set; setting to {}".format(CC.CLUSTERID_ENV, CC.CLUSTERID_UNKNOWN))
        os.environ[CC.CLUSTERID_ENV] = CC.CLUSTERID_UNKNOWN

    ###
    ### Set up Gurobi
    ###
    # Add the appropriate Gurobi directory to the path. 
    # This must be done *before* 'import gurobi' is executed.
    # Note: we also need to do this on the executors 
    if C.GUROBI_PATH in config[C.GUROBI_SECTION]:
        if 'gurobipy' in sys.modules:
            raise RuntimeError("Gurobipy has already been imported. It should only be imported in engine modules.")
        os.environ[CC.PYTHON_VERSION] = f'python{sys.version_info.major}.{sys.version_info.minor}'
        gurobi_path = os.path.expandvars(config[C.GUROBI_SECTION][C.GUROBI_PATH])
        sys.path.insert(0, gurobi_path)

    import gurobipy as gb  # Seems unused but probably needed by bom / make release / switching versions?

    ###
    ### Set up the Mission. It may be already set; if so, use it
    ###

    mission_name = dashboard.get_mission_name()
    display_mission(mission_name)
    dashboard.das_log(mission_name + ' starting', extra={'start':'now()'})


    ###
    ### Create the DAS object 
    ###

    das = das_framework.driver.main_make_das(args, config)

    # Have all annotations go to das_log, but no need to print, because annotations print
    das.add_annotation_hook(lambda message: programs.dashboard.das_log(message, verbose=False))

    # Create the sourcecode release and give it the same name as the logfile, except change
    # ".log" to ".release.zip"
    assert das.logfilename.endswith(".log")
    release_zipfilename = re.sub(r".log$", C.RELEASE_SUFFIX, das.logfilename)
    assert not os.path.exists(release_zipfilename)
    cmd = [sys.executable, __file__, '--logfile', '/dev/null', '--make_release', release_zipfilename, args.config]
    logging.info("Creating release: {}".format(" ".join(cmd)))
    subprocess.check_call(cmd)
    assert os.path.exists(release_zipfilename)

    ###
    ### Set up Logging and instrumentation
    ###

    # Make sure Spark log file directories exist
    for option in [C.SPARK_LOCAL_DIR, C.SPARK_EVENTLOG_DIR]:
        try:
            os.makedirs(config[C.SPARK][option], exist_ok=True)
        except KeyError as e:
            pass

    heartbeat_frequency = config.getint(section=C.GUROBI_SECTION,option=CC.HEARTBEAT_FREQUENCY,fallback=CC.HEARTBEAT_FREQUENCY_DEFAULT)
    if heartbeat_frequency>0:
        heartbeat_thread = threading.Thread(target=dashboard_heartbeat, name='heartbeat', 
                                            daemon=True, kwargs={'config':config})
        heartbeat_thread.start()

    notification_frequency = config.getint(section=C.GUROBI_SECTION,option=CC.NOTIFICATION_FREQUENCY,fallback=CC.NOTIFICATION_FREQUENCY_DEFAULT)
    stat_dict_list   = []       # list of all the stat dicts that we get
    syslog_watcher   = threading.Thread(target=das_performance.syslog_monitoring.monitor_appId,
                                        daemon=True,
                                        name='syslog_watcher',
                                        kwargs={'appId':applicationId, 'existing':False, 'tail':True, 'stat_dict_list':stat_dict_list,
                                                'update_frequency':notification_frequency })
    syslog_watcher.start()

    ###
    ###
    try:
        if das_framework.driver.strtobool(config[CC.STATS_SECTION][CC.NOTIFY_GC_MASTER]):
            gc.callbacks.append(gc_stats.gc_callback)
            logging.info("Logging GC stats")
    except KeyError as e:
        pass

    ###
    ### Populate the Logs
    ###

    # put the applicationId into the environment and log it
    dashboard.das_log(extra={CC.APPLICATIONID: applicationId}, log_spark=True)
    logging.info(json.dumps({CC.APPLICATIONID: applicationId,
                             CC.START: time.time(),
                             CC.FUNC: ' __main__'}))

    # Dump the spark configuration
    if cspark.spark_running():
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        for (key, val) in spark.sparkContext.getConf().getAll():
            logging.info("{}={}".format(key, val))

    # Save the git commit SHA-1 hash to the config file, so we know on
    # which commit it ran 
    #
    # WARNING: It is not guaranteed to get the same code that ran,
    # since files can be modified and not committed at the run time
    # (should add message)

    if config.getboolean(C.WRITER, C.SAVE_GIT_COMMIT, fallback=False):
        curdir = os.getcwd()
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
        commit_point = subprocess.run([C.GIT, C.SHOW], stdout=subprocess.PIPE).stdout.decode(C.UTF_8).split("\n")[0]
        if len(subprocess.run([C.GIT, "status", "--porcelain", "--untracked-files=no"], stdout=subprocess.PIPE).stdout) > 0:
            commit_point = commit_point + " (modifications from commit present at run time)"
        elif len(subprocess.run([C.GIT, "status", "--porcelain"], stdout=subprocess.PIPE).stdout) > 0:
            commit_point = commit_point + " (untracked files present at run time)"
        os.chdir(curdir)
        das.config.set(C.READER, C.GIT_COMMIT, commit_point)

    ### 
    ### Run the DAS
    ###

    das_framework.driver.main_run_das(das)

    ###
    ### Start the orderly shutdown
    ###

    logging.info(json.dumps({CC.APPLICATIONID: applicationId,
                             CC.TERMINATED: True,
                             CC.STOP: time.time(),
                             CC.FUNC: ' __main__'}))

    # terminate the DFXML writer
    if das.dfxml_writer.filename:
        das.dfxml_writer.exit() 
        das_stats.upload(das.dfxml_writer.filename) 

    total_seconds = int(das.running_time())
    minutes = int(total_seconds / 60)
    seconds = total_seconds % 60

    das.annotate("collected {} stats from syslog".format(len(stat_dict_list)))
    if config.getboolean(C.WRITER_SECTION, C.UPLOAD_LOGFILE, fallback=False):
        das.annotate("Uploading Logfile and Analysis Report")
        url = das_stats.run_stats_for_logfile(dfxml=das.dfxml_writer.filename, config=config, stat_dict_list=stat_dict_list)
        das.log_and_print("DAS config: {}".format(args.config))
        das.log_and_print("DAS run stats: {}".format(url))

    if das.output_paths:
        das.log_and_print("\nDAS OUTPUT FILES:")
        for path in das.output_paths:
            das.log_and_print('    '+path)
        das.log_and_print("")

    dashboard.das_log(mission_name + ' finished', extra={'stop': 'now()'})
    das.log_and_print(f"{sys.argv[0]}: Elapsed time: {total_seconds} seconds ({minutes} min, {seconds} seconds)")
