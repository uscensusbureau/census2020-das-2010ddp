#!/usr/bin/env python3
#
# cluster_info.py:
# A module of useful EMR cluster management tools.
# We've had to build our own to work within the Census Environment
# This script appears in:
#   das-vm-config/bin/cluster_info.py
#   emr_stats/cluster_info.py
#
# Currently we manually sync the two; perhaps it should be moved to ctools.

import os
import sys
from pathlib import Path
import json
import urllib.request

import subprocess
from subprocess import Popen,PIPE,call,check_call,check_output
import multiprocessing
import time
import logging

# Bring in aws from the current directory
sys.path.append( os.path.dirname(__file__))
import aws

# Beware!  An error occurred (ThrottlingException) in complete_cluster_info()
# when calling the ListInstances operation (reached max retries: 4): Rate exceeded
#
# We experienced throttling with DEFAULT_WORKERS=20
#
# So we use 4
DEFAULT_WORKERS=4

# We also now implement exponential backoff 
MAX_RETRIES = 8
RETRY_MS_DELAY = 50


debug = False

# Bring in ec2. It's either in the current directory, or its found through
# the ctools.ec2 module

try:
    import ec2
except ImportError as e:
    try:
        sys.path.append( os.path.dirname(__file__) )
        import ec2
    except ImportError as e:
        raise RuntimeError("Cannot import ec2")

# Proxy is controlled in aws

_isMaster  = 'isMaster'
_isSlave   = 'isSlave'
_clusterId = 'clusterId'
_diskEncryptionConfiguration='diskEncryptionConfiguration'
_encryptionEnabled='encryptionEnabled'

Status='Status'


def show_credentials():
    subprocess.call(['aws','configure','list'])

def get_url(url):
    with urllib.request.urlopen(url) as response:
        return response.read().decode('utf-8')

def user_data():
    """user_data is only available on EMR nodes. Otherwise we get an error, which we turn into a FileNotFound error"""
    try:
        return json.loads(get_url("http://169.254.169.254/2016-09-02/user-data/"))
    except json.decoder.JSONDecodeError as e:
        pass
    raise FileNotFoundError("user-data is only available on EMR")

def encryptionEnabled():
    return user_data()['diskEncryptionConfiguration']['encryptionEnabled']

def isMaster():
    """Returns true if running on master"""
    return user_data()['isMaster']

def isSlave():
    """Returns true if running on master"""
    return user_data()['isSlave']

def decode_status(meminfo):
    return { line[:line.find(":")] : line[line.find(":")+1:].strip() for line in meminfo.split("\n") }

def clusterId():
    return user_data()['clusterId']

def get_instance_type(host):
    return run_command_on_host(host,"curl -s http://169.254.169.254/latest/meta-data/instance-type")

# https://docs.aws.amazon.com/general/latest/gr/api-retries.html
def aws_emr_cmd(cmd):
    """run the command and return the JSON output. implements retries"""
    for retries in range(MAX_RETRIES):
        try:
            rcmd = ['aws','emr','--output','json'] + cmd
            if debug:
                print(f"aws_emr_cmd pid{os.getpid()}: {rcmd}")
            res = check_output(rcmd, encoding='utf-8')
            return json.loads(res)
        except subprocess.CalledProcessError as e:
            delay = (2**retries * (RETRY_MS_DELAY/1000))
            logging.warning(f"aws emr subprocess.CalledProcessError. "
                            f"Retrying count={retries} delay={delay}")
            time.sleep(delay)
    raise RuntimeError("MAX_RETRIES {} reached".format(MAX_RETRIES))
    

def list_clusters(*,state=None):
    """Returns the AWS Dictionary of cluster information"""
    cmd = ['list-clusters']
    if state is not None:
        cmd += ['--cluster-states',state]
    data = aws_emr_cmd(cmd)
    return data['Clusters']

def describe_cluster(clusterId):
    data = aws_emr_cmd(['describe-cluster','--cluster',clusterId])
    return data['Cluster']    

def list_instances(clusterId = None):
    if clusterId is None:
        clusterId = clusterId()
    data = aws_emr_cmd(['list-instances','--cluster-id',clusterId])
    return data['Instances']    

def add_cluster_info(cluster):
    clusterId = cluster['Id']
    debug = cluster['debug']
    if debug:
        print("Getting info for cluster",clusterId)
    cluster['describe-cluster'] = describe_cluster(clusterId)
    cluster['instances']        = list_instances(clusterId)
    cluster['terminated']       = 'EndDateTime' in cluster['Status']['Timeline']
    # Get the id of the master
    if debug:
        print("Cluster",clusterId,"has ",len(cluster['instances']),"instances")
    try:
        masterPublicDnsName = cluster['describe-cluster']['MasterPublicDnsName']
        masterInstance      = [i for i in cluster['instances'] if i['PrivateDnsName']==masterPublicDnsName][0]
        masterInstanceId    = masterInstance['Ec2InstanceId']
        # Get the master tags
        cluster['MasterInstanceTags'] = {}
        for tag in ec2.describe_tags(resourceId=masterInstanceId):
            cluster['MasterInstanceTags'][tag['Key']] = tag['Value']
        if debug:
            print("Cluster",clusterId,"got tags")
    except KeyError as e:
        pass
    return cluster


def complete_cluster_info(workers=DEFAULT_WORKERS,terminated=False,debug=False):
    """Pull all of the information about all the clusters efficiently using the
    EMR cluster API and multithreading. If terminated=True, get
    information about the terminated clusters as well.
    """
    if debug:
        print("Getting cluster list...")
    clusters = list_clusters()
    for cluster in clusters:
        cluster['debug'] = debug
    if debug:
        print("Cluster list: {} clusters".format(len(clusters)))
    if terminated==False:
        for cluster in list(clusters):
            if cluster['Status']['State'] in ('TERMINATED','TERMINATED_WITH_ERRORS'):
                clusters.remove(cluster)
        if debug:
            print("Cluster list reduced to {} clusters".format(len(clusters)))
    if debug:
        print(clusters)
    with multiprocessing.Pool(workers) as p:
        clusters = p.map(add_cluster_info,clusters)

    return clusters

