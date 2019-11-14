#!/usr/bin/env python3

"""
Functions for working with garbage collection performance.
Includes a test program to learn things.
"""

import gc
import logging 


def gc_callback(phase,info):
    logging.warning(f"GC phase={phase} info={info} objects={len(gc.get_objects())}")
    #print(f"gc_callback: phase={phase} info={info}")
    #print("gc.get_count:",gc.get_count())
    #print("gc.get_stats:",gc.get_stats())
    #print("len(gc.get_objects):",len(gc.get_objects()))
    #print()

if __name__=="__main__":
    print("gc.callbacks:",gc.callbacks)
    gc.callbacks.append(gc_callback)
    print("making some data")
    for k in range(1000000):
        root = [range(i,1000) for i in range(1,1000)]
    print("done")
