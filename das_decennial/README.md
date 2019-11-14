# DAS System Configuration

Internally, the das-vm-config repo handles setting up configuration requirements on emr cluster.

TODO: add relevant info for external use.

## Gurobi setup

## Spark setup

## Python setup

# Run commands

## On an EMR cluster
    $ sudo -u hadoop bash
    $ [config=configfile] [output=outputfile] bash run_cluster.sh

## On research 2

    $ qsub [-v config=configfile] dasrun2.qsub

# External Setup

    $ git clone git@github.com:dpcomp-org/das_decennial.git
    $ bash das_decennail/etc/setup_external
