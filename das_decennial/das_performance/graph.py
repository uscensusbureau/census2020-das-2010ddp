#!/usr/bin/env python3
# -*- mode: python; -*-

# 
# Graph the stats object
#
# Before anything else, set the Agg backend

import tempfile
import socket
import das_framework.ctools.tydoc as tydoc

from das_performance.das_stats import *

import matplotlib
from matplotlib.figure import Figure
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas

X_FONTSIZE=5
X_ROTATION=60

class EMR_Graph:
    def __init__(self,*,ci,dl=None):
        # However, setting Agg creates temporary files.
        # We do not want to do this on the Spark Core nodes, so we import matplotlib here
        #
        #
        # Continue the rest of the initialization
        #
        self.ci       = ci      # cluster info structure
        self.tf       = None    # LaTeX file
        self.tmpdir   = tempfile.TemporaryDirectory()
        self.dl       = dl
        if self.dl:
            self.annotations = dl.annotations
        else:
            self.annotations = []

    def sorted_nodes(self):
        return sorted(self.ci.nodes.values(), key=lambda node:node.instanceID)

    def graph_cluster_load(self,doc):
        print("graph_cluster_load")
        fig = Figure()
        canvas = FigureCanvas(fig)
        ax1 = fig.add_subplot(111)
        fig.set_size_inches(6,6)

        ax1.xaxis_date()
        ax1.set_axisbelow(True)
        #self.plt.setp( ax1.xaxis.get_majorticklabels(), rotation=X_ROTATION, fontsize=X_FONTSIZE)
        ax1.set_ylabel('System Load')

        ax2 = ax1.twinx()
        ax2.set_ylabel('Normalized System Load')
        all_ax2_yvalues = []

        for node in self.sorted_nodes():
            # Not every stat has all of the data that we want to graph, so we need to extract the tuple for each,
            # and add them to the list. If one is missing a value, we will get an exception, and that stat will not be
            # included
            data = []
            for stat in node.stats:
                print(stat)
                try:
                    data.append((stat[DATETIME], stat[LOAD_AVERAGE][0], stat[LOAD_AVERAGE][0]/stat[CORES]))
                except KeyError as e:
                    pass
            if data:
                data.sort()
                dates  = [d[0] for d in data]
                avgs   = [d[1] for d in data]
                navgs  = [d[2] for d in data]

                assert len(dates) == len(avgs)
                assert len(dates) == len(navgs)

                ax1.plot( dates, avgs, label=node.instanceID + " " + self.ci.instance_type(node.instanceID), alpha=0.7)
                ax2.plot( dates, navgs, label=node.instanceID + " " + self.ci.instance_type(node.instanceID), alpha=0.7)
                all_ax2_yvalues.extend(navgs)

        if not all_ax2_yvalues:
            print("No cluster load information to graph")
            doc.p("No cluster load information to graph")
            return

        ax2_ymin = min(all_ax2_yvalues)
        ax2_ymax = max(all_ax2_yvalues)
        textoffset = 0.2
        for (when,what) in self.annotations:
            ax2.add_line( matplotlib.lines.Line2D([when,when] , [ax2_ymin, ax2_ymax] ) )
            ax2.text( when, ax2_ymin + (ax2_ymax-ax2_ymin) * textoffset, what, horizontalalignment='center', fontsize=10.0,  )
            textoffset += 0.1
            if textoffset > 0.8:
                textoffset = 0.2

        ax1.legend(loc='best',fontsize=4)
        doc.p("Per-node CPU Utilization:")
        doc.insert_matplotlib(fig)
        t = doc.table()
        t.add_head(["Node","Samples"])
        for node in self.sorted_nodes():
            if node.stats:
                t.add_data([node.instanceID,len(node.stats)])

    def graph_cluster_memory(self,doc):
        fig = Figure()
        canvas = FigureCanvas(fig)
        ax = fig.add_subplot(111)

        ax.xaxis_date()
        ax.set_axisbelow(True)
        ax.set_ylabel('% Free Memory per node')
        ax.set_ylim(bottom=0,top=1.0)
        count = 0
        for node in self.sorted_nodes():
            data = []
            for stat in node.stats:
                try:
                    data.append((stat[DATETIME], stat[MEM_STATS]['free']/stat[MEM_STATS]['total']))
                except KeyError as e:
                    pass
            if data:
                data.sort()
                dates = [d[0] for d in data]
                mems  = [d[1] for d in data]
                ax.plot( dates, mems, label=node.instanceID + " " + self.ci.instance_type(node.instanceID))
                count += len(data)
        if count==0:
            print("No cluster memory information to graph")
            doc.p("No cluster memory info.")
            return
        doc.p("Per-node Memory Utilization:")
        ax.legend(loc='best',fontsize=4)
        #self.plt.setp( ax.xaxis.get_majorticklabels(), rotation=X_ROTATION, fontsize=X_FONTSIZE)
        doc.insert_matplotlib(fig)

    def write_cluster_messages(self,doc):
        # First collect the kernel messages
        kmessages = []
        for node in self.sorted_nodes()[0:1]:
            first = True
            for obj in node.stats:
                for (when,what) in obj[MESSAGES]:
                    if first:
                        kmessages.append(f"Instance {node.instanceID}:")
                        first = False
                    kmessages.append(f"{when} {what}")
        
        if kmessages:
            doc.p("Kernel messages:")
            doc.pre("\n".join(kmessages))
        else:
            doc.pre("No kernel messages:\n\n")
            
    def describe_cluster(self,doc):
        doc.p("Cluster Description:")
        doc.pre( json.dumps(self.ci.describe_cluster, sort_keys=True, indent=4))
        for node in self.sorted_nodes()[0:1]:
            doc.pre( json.dumps(node.instanceInfo, sort_keys=True, indent=4))
            
    def insert_graph_data(self,doc,describe_cluster=False):
        print("insert graph data")
        if self.dl:
            doc.h2("Configuration")
            t = doc.table()
            t.add_data(['DAS config file:',self.dl.config_file])
            t.add_data(['Run end:',self.dl.last_date.isoformat()])

        self.graph_cluster_load(doc)
        self.graph_cluster_memory(doc)
        self.write_cluster_messages(doc)
        if describe_cluster:
            self.describe_cluster(doc)
        #doc.render(sys.stdout,format='html')
