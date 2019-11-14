#!/usr/bin/evn python3
"""Gurobi statistics from syslog"""

import datetime
import time
import logging
import dateutil.parser
import json
import os
import re 
import sys
import xml.etree.ElementTree as ET

import numpy as np
import pandas as pd
import subprocess

# This is to support standalone operation
try:
    import das_framework
    import das_performance
except ImportError:
    sys.path.append( os.path.dirname(__file__))
    sys.path.append( os.path.join(os.path.dirname(__file__), ".."))


TERMINATED="terminated"
LOC_POS = 'best'


import constants as C
from constants import CC
from das_framework.ctools.timer import Timer
from das_framework.ctools import tydoc

import matplotlib
from matplotlib.figure import Figure
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas

def extract_syslog_jsondicts_for_appId(*,logfile,appId=None,existing=False,tail=False):
    """Generator to return all of the JSON objects in a logfile for a particular appId. We use grep as a filter because it is so much faster than doing it in python. 
    If existing=True, scan the existing file.
    If tail=True, then run tail -f on the file. It will never return, so you should run this in another thread
    """
    cmds = []
    if existing:
        if appId:
            cmds.append(['grep',appId,logfile])
        else:
            cmds.append(['grep','[{]',logfile])
        
    if tail:
        cmds.append(['tail','-f',logfile])
    
    for cmd in cmds:
        p = subprocess.Popen(cmd,stdout=subprocess.PIPE,encoding='utf-8')
        for line in p.stdout:
            if line=='':
                break
            if (appId is not None) and (appId not in line):
                continue
            if "terminated" in line:
                yield {'applicationId':appId,TERMINATED:True}
                return
            m = CC.FIND_JSON_RE.search(line)
            if not m:
                continue
            try:
                obj = json.loads(m.group(1))
                if 'host' not in obj:
                    obj['host'] = line.split()[3]
                yield obj
            except json.decoder.JSONDecodeError as e:
                print(f"JSON Decode error {e} in JSON: {m.group(1)}")

class GurobiSyslog():
    def __init__(self, *, doc):
        self.doc        = doc
        self.df         = None

    #Create a lookup function to assign a discrete stage in the process.
    OPTIMIZER_POINT_TO_STEP = {
        ("L2PlusRounderWithBackup",CC.POINT_ENV_CREATE) : 0,
        ("L2PlusRounderWithBackup",CC.POINT_FAILSAFE_START) :98,
        ("L2PlusRounderWithBackup",CC.POINT_FAILSAFE_END):99,
        ("L2GeoOpt",CC.POINT_MODBUILD_END) :11,
        ("L2GeoOpt",CC.POINT_PRESOLVE_START) :12,
        ("L2GeoOpt",CC.POINT_PRESOLVE_END) :13,
        ("L2GeoOpt",CC.POINT_OPTIMIZE_END) :14,
        ("L2GeoOpt",CC.POINT_FAILSAFE_START) :18,
        ("L2GeoOpt",CC.POINT_FAILSAFE_END) :19,
        ("GeoRound",CC.POINT_MODBUILD_END) :21,
        ("GeoRound",CC.POINT_PRESOLVE_START) :22,
        ("GeoRound",CC.POINT_PRESOLVE_END) :23,
        ("GeoRound",CC.POINT_OPTIMIZE_END) :24,
        ("GeoRound",CC.POINT_FAILSAFE_START) :28,
        ("GeoRound",CC.POINT_FAILSAFE_END) :29 }

    def createDataFrame(self,dictlist):
        """Method create a pandas dataframe from a list of statistics dictionaries.
        Performs the recode on each dictionary and then creates the pandas dataframe as a single operation.

        This method creates a step varialble using the AssignStep
        method to represent the optimization i.e., environment
        creation, L2, and L1 optimizations, and the point in the
        optimization, i.e. model build, presolve, and model
        optimization.

        Returns a pandas dataframe. 
        """

        # All columns are objects except for the ones that we convert to floats:
        FLOAT_COLS = set(['t','t_env_start','t_env_end','t_modbuild_start','t_modbuild_end','t_presolve_start',
                          't_presolve_end','t_optimize_start','t_optimize_end','loadavg','utime','stime',
                          'maxrss','utime_children','stime_children'])

        def makeFloat(v):
            if v is None:
                return None
            return float(v)

        def recodeDict(olddict):
            newdict = {k:makeFloat(v) if k in FLOAT_COLS else v for (k,v) in olddict.items()}
            newdict[C.OPTIMIZER] = newdict[C.OPTIMIZER].split('.')[-1].replace("'>","")
            for key in ['stack','Runtime','Status','parentGeocode']:
                if key in newdict:
                    del newdict[key]
            newdict['node'] = newdict['instanceId'][-4:] 
            newdict['step'] = self.OPTIMIZER_POINT_TO_STEP[ (newdict['optimizer'], newdict['point']) ]
            return newdict

        # Recode the entire list. Only use dictionaries that have C.OPTIMIZER in them

        dictlist = [  recodeDict(olddict) for olddict in dictlist if C.OPTIMIZER in olddict]
        self.df = pd.DataFrame(dictlist)

    def parselogfile(self,*,fname=CC.LOCAL1_LOG_PATH, appId):
        """Parse a log filename for a specific appId"""
        dictlist = list( extract_syslog_jsondicts_for_appId(logfile=fname, appId=appId, existing=True) )
        self.createDataFrame(dictlist)

    def recodedata(self):
        '''Function carries the environment start/end times tTho other records
        for the geounit. Given the nature of the failsafe recording,
        this flags all logs occuring between the beginning and ending
        of the failsafe as having occured during the failsafe.
        Finally, the data are sorted by geocode and the process that
        the long was written.
        '''

        df = self.df
        df.sort_values(by=['geocode','t'])
        df.reset_index(drop=True)

        df['failsafeon']=0        
        geocode_1=""
        step_1=""
        failsafeon=0

        st = time.time()
        for i in range(0,df.shape[0]):
            if not pd.isnull(df.iloc[i,:]['geocode']):
                #When switching to new geocode
                if not geocode_1==df.geocode[i]:
                    failsafeon=0
                    step_1=""
                    geocode_1=df.geocode[i]

                    #Save the environment times   
                    if df.step[i]==0:
                        env_start=df.t_env_start[i]
                        env_end=df.t_env_end[i]

                    if df.step[i]==CC.POINT_FAILSAFE_START:
                        failsafeon=1
                    else:
                        if failsafeon==1:
                            df.loc[i,'failsafeon']=1

                #Cases where geocode doesn't change
                else:
                    if df.point[i]==CC.POINT_FAILSAFE_START:
                        failsafeon=1
                    elif df.point[i]==CC.POINT_FAILSAFE_END:
                        failsafeon=0
                    else:
                        if failsafeon==1:
                            df.loc[i,'failsafeon']=1

                    #Save the environment times
                    if df.step[i]==0:
                        env_start=df.t_env_start[i]
                        env_end=df.t_env_end[i]

                    df.loc[i,'t_env_start']=env_start
                    df.loc[i,'t_env_end']=env_end   

        et=round(time.time()-st,0)
        self.df = df

        print('Log sorting/recoding took {} seconds'.format(et))   
    
    def makeCDFs(self):
        data = self.df

        self.doc.h1("CDFs of system metrics, by node")
        '''This method adds CFS to the document object that follow the pattern
        of the variable's metric on the y axis and a cumulative
        percentage of geounits that have that value or lower.

        These graphs are disaggregated by node and the node color will
        scale automatically based on the colormap object. The color
        map can be changed easily. The parameters for the different
        graphs are defined in graphstobuild object.

        '''
    
        # Call method to parse spark log for maximum memory
        # FIXME!
        
        st=time.time()
        
        #Positions: 1) column name, 2) how to aggregate by geounit, 3) y title, 4) plot title, 5) specialized handling or recoding
        graphstobuild=(        
            ('maxrss','max','Percentage used','Percent memory utilized','maxmemory'),
            ('loadavg','max','Avg # of threads','Average number of threads in run state','na'),
            ('utime','max','Time, in seconds','User time','na'),
            ('stime','max','Time, in seconds','System time','na'),
            ('utime_children','max','Time, in seconds','User time for children processes','na'),
            ('stime_children','max','Time, in seconds','System time for children processes','na'),
            ('maxrss_children','max','Percentage used','Percent memory utilized by children processes','maxmemory')
        )

        # Begin looping through the different variables to graph

        import matplotlib.colors

        for i in range(0,len(graphstobuild)):
            parms=graphstobuild[i]
            nodelist=self.df.node.unique()
            
            #Construct the graphs by node
            colormap = matplotlib.cm.get_cmap('gist_rainbow')
            colors=[colormap(i) for i in np.linspace(0, 0.9, len(nodelist))]
            labels=[]
           
            fig = Figure()
            canvas = FigureCanvas(fig)
            ax = fig.add_subplot(111)
            #Begin looping through the graph
            for k in range(0,len(nodelist)):
                node=nodelist[k]

                #Y values, sorted and subset by node
                df=self.df.loc[self.df.node==node,['geocode','childGeolevel',parms[0]]]
                dfgrouped=df.groupby(['geocode','childGeolevel'])

                #Collapse to one unit per geocode by max/mean/sum
                if parms[1]=='max':
                    y=dfgrouped.max()
                if parms[1]=='sum':
                    y=dfgrouped.sum()
                if parms[1]=='mean':  
                    y=dfgrouped.mean()

                #Sort values and return as list
                y.sort_values(parms[0], axis = 0, ascending = True, inplace = True, na_position ='last') 
                y=list(y.iloc[:,0])

                #If adjustment necessary to y value, make it here
                if parms[4]=='maxmemory':
                    #if node in maxmemory.index.get_values():
                    #    nodemaxmemory=maxmemory[node] / 1000
                    #else: 
                    #    print("Error: Node {} does not have associated memory statistics in spark log. Using large value.".format(node))
                    #    nodemaxmemory=99999999999 / 1000

                    #y=[round((value / nodemaxmemory)*100 , 2) for value in y]
                    y = [value for value in y]

                #Create x=axis percentage
                x=[(i/len(y))*100 for i in range(1,len(y)+1)]

                #Plot the data
                ax.plot( x, y, linestyle='-', color=colors[k])
                labels.append('{}'.format(node)) 
                                
            ax.set_title('{0}'.format(parms[3]))
            ax.set_ylabel('{}'.format(parms[2]))
            ax.set_xlabel('Percentage geounits completed')        

            ax.legend(labels, ncol=4, loc=LOC_POS,
                      columnspacing=1.0, labelspacing=0.0,
                      handletextpad=0.0, handlelength=1.5,fancybox=True)

            self.doc.append_matplotlib(fig,pad_inches=0.1)

        et=round(time.time()-st,0)
        print('Make CDFs took {} seconds'.format(et))
   
    def durationgraphs(self,nodes=False,geolevels=False,debug=False):

        data = self.df
        st=time.time()
        
        if (not nodes) & (not geolevels):
            print('Error in duration graphs: By value not selected for duration graphs. Use argument nodes=True or geolevels=True')
            
        elif nodes & geolevels:
            print('Error in duration graphs: Too many by arguments selected. Use argument only one of the argument, nodes=True or geolevels=True')

        else:
            #Define the paramters for plotting figures 
            '''
            graphstobuild=(         
            ((data.step==24) & (data.t_env_start!="") & (data.failsafeon==0),'t_env_start','t_optimize_end','Complete Run, no fail safe'), 
            ((data.step==24) & (data.t_env_start!="") & (data.failsafeon==1),'t_env_start','t_optimize_end','Complete Run, with fail safe')
            )
            
            '''
                
            graphstobuild=(        
                ((data.step==0)  & (data.failsafeon==0),'t_env_start','t_env_end','Environment creation'),
                ((data.step==11) & (data.failsafeon==0),'t_modbuild_start','t_modbuild_end','L2GeoOpt - L2 Model build, no fail safe'),
                ((data.step==13) & (data.failsafeon==0),'t_presolve_start','t_presolve_end','L2GeoOpt - L2 Presolve, no fail safe'),
                ((data.step==14) & (data.failsafeon==0),'t_optimize_start','t_optimize_end','L2GeoOpt - L2 Optimization, no fail safe'),
                ((data.step==21) & (data.failsafeon==0),'t_modbuild_start','t_modbuild_end','Georound - L1 Model build, no fail safe'),
                ((data.step==23) & (data.failsafeon==0),'t_presolve_start','t_presolve_end','Georound - L1 Presolve, no fail safe'),
                ((data.step==24) & (data.failsafeon==0),'t_optimize_start','t_optimize_end','Georound - L1 Optimization, no fail safe'), 
                ((data.step==24) & (data.failsafeon==0),'t_env_start','t_optimize_end','Complete Run, no fail safe'),
                ((data.step==0)  & (data.failsafeon==1),'t_env_start','t_env_end','Environment creation, should be none here'),
                ((data.step==11) & (data.failsafeon==1),'t_modbuild_start','t_modbuild_end','L2GeoOpt - L2 Model build, with fail safe'),
                ((data.step==13) & (data.failsafeon==1),'t_presolve_start','t_presolve_end','L2GeoOpt - L2 Presolve, with fail safe'),
                ((data.step==14) & (data.failsafeon==1),'t_optimize_start','t_optimize_end','L2GeoOpt - L2 Optimization, with fail safe'),
                ((data.step==21) & (data.failsafeon==1),'t_modbuild_start','t_modbuild_end','Georound - L1 Model build, with fail safe'),
                ((data.step==23) & (data.failsafeon==1),'t_presolve_start','t_presolve_end','Georound - L1 Presolve, with fail safe'),
                ((data.step==24) & (data.failsafeon==1),'t_optimize_start','t_optimize_end','Georound - L1 Optimization, with fail safe'), 
                ((data.step==24) & (data.failsafeon==1),'t_env_start','t_optimize_end','Complete Run, with fail safe')
            )
           
            #print(df[['geocode','step','t_env_start','t_env_end','t_modbuild_start','t_modbuild_end','t_presolve_start','t_presolve_end','t_optimize_start','t_optimize_end']].head(30))
            
            if nodes:
                self.doc.h1("CDFs of optimization duration times, by node")
            if geolevels:
                self.doc.h1("CDFs of optimization duration times, by geographic level")           

            #Loop through graphing parameters
            for i in range(0,len(graphstobuild)):
                parms=graphstobuild[i]

                #If disaggregate by nodes
                if nodes:
                    nodelist=data.node.unique()

                    #Construct the graphs by node
                    colormap = matplotlib.cm.get_cmap('gist_rainbow')
                    colors=[colormap(i) for i in np.linspace(0, 0.9, len(nodelist))]
                    labels=[]

                    fig = Figure()
                    canvas = FigureCanvas(fig)
                    ax = fig.add_subplot(111)
                    #Begin looping through NODES
                    graphed=False
                    for k in range(0,len(nodelist)):
                        node=nodelist[k]

                        #Calculate y data
                        subset=parms[0] & (data.node==node)
                        y=data.loc[subset,parms[2]]-data.loc[subset,parms[1]]
                        y=list(y.sort_values())
                        if debug:
                            print('Duration graphs: map paramters={0}, node={1}, n records={2}'.format(i,node,len(y)))
                        
                        if len(y)!=0:
                            #Calculate x data
                            x=[(i/len(y))*100 for i in range(1,len(y)+1)]
                            ax.plot( x, y, linestyle='-', color=colors[k])
                            labels.append('{}'.format(node)) 
                            graphed=True
                        
                    ax.set_title('{0}'.format(parms[3]))
                    ax.set_ylabel('Time (seconds)')   
                    ax.set_xlabel('Percentage geounits')
                    ax.legend(labels, ncol=4, loc=LOC_POS,
                              columnspacing=1.0, labelspacing=0.0,
                              handletextpad=0.0, handlelength=1.5,fancybox=True)
                
                    if graphed:
                        self.doc.append_matplotlib(fig,pad_inches=0.1)  
                
                #Make graphs disaggregated by geolevels
                if geolevels:

                    geolist=data.childGeolevel.unique()

                    #Construct the graphs by node
                    colormap = matplotlib.cm.get_cmap('gist_rainbow')
                    colors=[colormap(i) for i in np.linspace(0, 0.9, len(geolist))]
                    labels=[]
                    
                    fig = Figure()
                    canvas = FigureCanvas(fig)
                    ax = fig.add_subplot(111)
                    #Begin looping through NODES
                    graphed=False
                    for k in range(0,len(geolist)):
                        geolevel=geolist[k]

                        #Calculate y data
                        subset=parms[0] & (data.childGeolevel==geolevel)
                        y=data.loc[subset,parms[2]]-data.loc[subset,parms[1]]
                        y=list(y.sort_values())
                        if debug:
                            print('Duration graphs: map paramters={0}, node={1}, n records={2}'.format(i,geolevel,len(y)))
                        if len(y)!=0:
                            #Calculate x data
                            x=[(i/len(y))*100 for i in range(1,len(y)+1)]

                            ax.plot( x, y, linestyle='-', color=colors[k])
                            labels.append('{}'.format(geolevel)) 
                            graphed=True

                    ax.set_title('{0}'.format(parms[3]))
                    ax.set_ylabel('Time (seconds)')   

                    ax.set_xlabel('Percentage geounits')
                    ax.legend(labels, ncol=4, loc=LOC_POS,
                           columnspacing=1.0, labelspacing=0.0,
                           handletextpad=0.0, handlelength=1.5,fancybox=True)  

                    if graphed:                           
                        self.doc.append_matplotlib(fig,pad_inches=0.1)  
        
            et=round(time.time()-st,0)
            
            if nodes:
                print('Graphs of DAS run times by nodes took {} seconds'.format(et))                           
            else:
                print('Graphs of DAS run times by geolevels took {} seconds'.format(et))                           

    def timegeolevel(self):
        '''Method produces a tydoc table with the start time, end time, and total time of the DAS run spent in each geolevel.'''
        st=time.time()
        
        df = self.df

        df=df[['childGeolevel','t_env_start','t_optimize_end']].groupby('childGeolevel')
        min=df.t_env_start.min()
        max=df.t_optimize_end.max()

        df=pd.concat([min, max], axis=1)
        df['timestart']=[datetime.datetime.fromtimestamp(i).strftime("%Y-%m-%d %H:%M:%S") for i in df.t_env_start]
        df['filler1']="&nbsp;&nbsp;"
        df['timeend']=[datetime.datetime.fromtimestamp(i).strftime("%Y-%m-%d %H:%M:%S") for i in df.t_optimize_end]
        df['filler2']="&nbsp;&nbsp;"
        df['totaltime']=df['t_optimize_end']-df['t_env_start']
        df['totaltime']=[str(round(x,0)) for x in df.totaltime]
        df=df.drop(['t_env_start','t_optimize_end'],axis=1)
        
        self.doc.h1('Optimization time of each geolevel')
        t = self.doc.table()
        t.set_fontsize(8)
        t.add_head(['Geolevel','Time start','','Time end','','Total time (seconds)'])
        #Adds data to the table
        for i in range(0,df.shape[0]):
            x=[df.index[i]]
            y=list(df.iloc[i,:])
            t.add_data(x + y)
        
        et=round(time.time()-st,0)
        print('Preparation of the optimization time by geolevel took {} seconds'.format(et))

    def reviewfailsafes(self):
        
        df=self.df[['childGeolevel','geocode','step','failsafeon']]

        failsafe_grouped=df.groupby(['childGeolevel','geocode'])
        failsafe_grouped=failsafe_grouped.failsafeon.max()
      
        #Number of fail safes by geolevel
        nfailsafes=failsafe_grouped.groupby(['childGeolevel']).sum()
        self.doc.h1("Number failsafes triggered per geolevel")
        t=self.doc.table()
        t.set_fontsize(8)
        t.add_head(['Geolevel','Number failsafes'])
        
        #Add data to the table
        failsafe_count = 0
        for i in range(0,nfailsafes.shape[0]):
            x=[nfailsafes.index[i]]
            y=[str(nfailsafes[i])]
            t.add_data(x + y)
            failsafe_count += nfailsafes[i]

        if failsafe_count==0:
            self.doc.p('Failsafe mechanism was not triggered')
            return
            
        # List of the geocode with fail safes 
        listfailsafe=pd.DataFrame(failsafe_grouped[failsafe_grouped==1])
        listfailsafe.reset_index(inplace=True)

        self.doc.h1("List of geocodes optimized with the failsafe")
        t=self.doc.table()
        t.set_fontsize(8)
        t.add_head(['Geolevel','Geocode'])
        for i in range(0,listfailsafe.shape[0]):
            t.add_data( list(listfailsafe.iloc[i,0:2]) )
   
    def cdfdas(self,*,nbins,normalize=True):
        '''Plot the gurobi runs in the time frame of the entire DAS run. Lines are separated by geolevel, and graphs are separated by node.
        normalize - when true, the metric of the x axis is 0-100, when false it's 0 to the total time spend in gurobi optimization.'''

        data = self.df


        time_start=min(data.t)-3
        time_end=max(data.t)+3    
        total_seconds=(time_end-time_start)
        print('total_seconds={}, time_start={}, time_end={}'.format(total_seconds,time_start,time_end))
        
        #Change the start and end time parameters to prevent out of range time stamps
        tempmax=max(data.t)   
        tempmin=min(data.t)
        if tempmax>time_end:
            time_end=tempmax
        if tempmin<time_start:
            time_start=tempmin        
       
        #Create bounds for working with time stamps
        #Bounds as seconds from start time
        upbound=[k * total_seconds/nbins for k in range(1,nbins+1)]
        lowbound=[k * total_seconds/nbins for k in range(0,nbins)]
        SecBounds=list(zip(lowbound,upbound))

        #Switch from seconds to timestamps
        upbound=[k + time_start for k in upbound]
        lowbound=[k + time_start for k in lowbound]
        TimeBounds=list(zip(lowbound,upbound))

        #Define an intervval object to create bins
        cutpoints=pd.IntervalIndex.from_tuples(TimeBounds)

        #Keep only the last step to identify completed optimization
        #Define the bin as the upper bound
        temp=data.loc[data.step==24,["childGeolevel",'node',"t"]]   
        temp['temp']=pd.cut(data.t,bins=cutpoints)
        temp['bin']=[x.right for x in temp.temp]

        #Convert the bin from a timestamp to a seconds since start measure
        #Either normalized as 0-100 percent of run or in seconds
        if normalize==True:
            self.doc.h1('Gurobi optimization times relative to enter DAS run, time normalized')
            temp.loc[:,'bin']=((((temp.bin - time_start) / total_seconds))*100) 
        else:
            self.doc.h1('Gurobi optimization times relative to enter DAS run, time in seconds')
            temp.loc[:,'bin']=((temp.bin - time_start) / 1) 

        #Create counts of each bin    
        tempgrouped=temp.groupby(['node','childGeolevel'])
        data=tempgrouped.bin.value_counts()

        #  Identify the geographic levels.
        SORT_ORDER=['National','State','County','EnumDist','Tract','Block_Group','Block']
        for childGeolevel in set([row[0] for row in list(data.index.levels[0])]):
            if childGeolevel not in SORT_ORDER:
                SORT_ORDER.append(childGeolevel)

        GeolevelList=list(data.index.get_level_values('childGeolevel').unique())        
        NodesList=list(data.index.get_level_values('node').unique())
        NodesList.append('all nodes')

        #Create new figure object
        fig = Figure()
        canvas = FigureCanvas(fig)
        axes = fig.subplots(nrows=len(NodesList))
        fig.subplots_adjust(hspace=.3)
        fig.set_size_inches(8,len(NodesList)*4)

        # Define the colors of the geolevels
        hues = {'National':0,
                'State':0.2,
                'County':0.4,
                'Enumeration District':0.5,
                'Tract':0.6,
                'Block_Group':0.8,
                'Block':0.9}    

        #Create the max and min bins for keeping plots on the same scale across nodes
        #i.e., set the min and max x values. 
        minbin=0
        if normalize==True:
            maxbin=100
        else:
            maxbin=total_seconds

        #Loop through nodes to create graphs
        pltrow=0
        for node in NodesList:
            if node=='all nodes':
                nodesub=list(data.index.get_level_values('node').unique())
            else:
                nodesub=node    

            df1=data.loc[nodesub].groupby(['childGeolevel','bin']).sum()

            listgeolevels=list(df1.index.get_level_values('childGeolevel').unique())
            labels=[]
            for childGeolevel in listgeolevels:
                #print(childGeolevel)

                #Create x axis                 
                x = [minbin] # Adds the first point
                x.append(min(list(df1.loc[childGeolevel].index)))  # Add second point to duplicate lowest in subset. Will set to 0 to prevent linear increase.
                x.extend(list(df1.loc[childGeolevel].index)) #All of the bins
                x.append(maxbin) #Adds the last point
                
                #counts completed, starts at 0.
                y = [0,0] #First two points are zero.
                total = 0                  
                for count_ in df1.loc[childGeolevel]: #Cumulative otals for each bin
                    total += count_
                    y.append(total)
                y.append(total) #Duplicate the last point
                
                #For cases where there are some records            
                if total>=0:
                    yvals = [(val/total)*100 for val in y] #Converts y to percentage
                    color = matplotlib.colors.hsv_to_rgb([ hues[childGeolevel], 1.0, 0.5])
                    label = ' {}'.format(childGeolevel)
                    axes[pltrow].plot( x, yvals, linestyle='-', color=color)

                    axes[pltrow].title.set_text('{0}'.format(node))
                    axes[pltrow].text(-.15,.5,'Percentage geounits completed',va='center',transform=axes[pltrow].transAxes,rotation='vertical')   
                    labels.append('{}'.format(childGeolevel)) 

                    if normalize==True:
                        axes[pltrow].set_xlabel('Percentage of DAS run')
                    else:
                        axes[pltrow].set_xlabel('Time from DAS start (seconds)')
                    axes[pltrow].legend(labels, ncol=4, loc=LOC_POS,
                           columnspacing=1.0, labelspacing=0.0,
                           handletextpad=0.0, handlelength=1.5,fancybox=True)
            #End of geolevel loop
            pltrow += 1
        #End of node loop

        #Add results
        self.doc.append_matplotlib(fig,dpi=72,pad_inches=0.1)

    def run(self,nbins=100):
        """Performs analysis; requires that the DF has been made"""
        if self.df is None:
            raise RuntimeError("You musat call parselogfile or createDataFrame first")

        #Sorts and recodes data
        self.recodedata()
    
        #Table the optimization time by geolevel
        self.timegeolevel()
    
        #Review failsafes
        self.reviewfailsafes()
    
        #Make the CFS of resources
        self.makeCDFs()
    
        #Make the duration graphs
        self.durationgraphs(nodes=True,geolevels=False) 
        self.durationgraphs(nodes=False,geolevels=True) 
        
        #Make the gurobi optimization time graph
        self.cdfdas(normalize=True,nbins=nbins)          
        self.cdfdas(normalize=False,nbins=nbins)          

        return self.doc
        
    def save(self,save_base):
        store = pd.HDFStore( save_base + ".h5" )
        store['df'] = self.df
        #self.df.to_pickle( save_base + ".df.pickle")
        self.df.to_csv( save_base + ".df.csv")

def compile_and_insert_graph_data(*,doc,dl,save_base=None):
    gbsys=GurobiSyslog(doc=doc)
    gbsys.parselogfile(appId=dl.appId)
    gbsys.run()
    if save_base is not None:
        gbsys.save(save_base)


def standalone_test(logfile):
    """Create a DASLOG object and generate the syslog"""

    import daslog
    dl = daslog.DASLogfile(daslog=logfile)
    
    # Open a document for 
    doc = tydoc.tydoc()

    # Run the method
    compile_and_insert_graph_data(dl=dl,doc=doc)
    
    #Save the document
    filename=logfile.replace('.log','.html')
    doc.save(filename)

    # Upload doc to website
    import subprocess
    jbid = os.environ['JBID']
    cmd = f'scp -p {filename} {jbid}@dasexperimental.ite.ti.census.gov:/var/www/html/c'
    print(cmd)
    subprocess.check_output(cmd, shell=True)
    print("Files copied to dasexperimental")


if __name__=="__main__":
    from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter,description='Test program to make a graph from a DAS logfile')
    parser.add_argument("logfile")
    args = parser.parse_args()
    standalone_test(args.logfile)
