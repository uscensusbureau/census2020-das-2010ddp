from otherconsts.schemaconst import _SchemaConst
from otherconsts.sf1_schemaconst import _SF1SchemaConst
from otherconsts.dhcp_schemaconst import _DHCPSchemaConst

""" Module for program-wide constants.

Usage in the calling program:

    from constants import C

Then to reference CONSTANT, just use:

    C.CONSTANT

Attempting to assign to C.CONSTANT will raise an error.

 """
__version__ = "0.1.0"
import re


# newstyle:

class _DAS_CONSTANTS:
    def __setattr__(self, name, value):
        raise TypeError(f"Attempt to set read/only constant {name}")

    # pylint: disable=bad-whitespace

    ###########################################
    # Environment Variables
    ###########################################
    DAS_S3ROOT_ENV = 'DAS_S3ROOT'
    CLUSTERID_ENV  = 'CLUSTERID'
    CLUSTERID_UNKNOWN = 'j-UNKNOWN'
    PYTHON_VERSION='PYTHON_VERSION'

    ###########################################
    # Analysis Constants
    ###########################################
    RUN_PREFIX = "run_"
    RUN_INFEAS = "feas_dict.json"
    RUN_DATA = "data/"

    ###########################################
    # Config Constants (Analysis)
    ###########################################
    ANALYSIS = "Analysis"
    ANALYSIS_SECTIOn = 'analysis'
    INPUT_PATH = "input_path"
    SUM = "sum"
    COLLECT = "collect"
    AGGTYPES = "aggtypes"
    GEOLEVELS = "geolevels"
    QUERIES = "queries"
    METRICS = "metrics"
    RUN_PREFIX_OPTION = "run_prefix"
    RUN_CONTENTS_OPTION = "run_contents"
    SCHEMA = "schema"
    GROUPS = "groups"
    ANALYSIS_GEODICT = "Geodict"
    NUMCORES = "numcores"
    RECALCULATE_RESULTS = "recalculate_results"
    REBUILD_RESMAS = "rebuild_resmas"
    ANALYSIS_LOG = "analysis_logfile"

    SLDL = "SLDL"
    SLDU = "SLDU"

    JOB_FLOW_JSON='/emr/instance-controller/lib/info/job-flow.json'
    LOCAL1_LOG_PATH = '/var/log/local1.log'
    DEFAULT_LOG_FILE =LOCAL1_LOG_PATH
    FIND_JSON_RE = re.compile(r"(\{.*\})")

    # when statistics are recorded they are associated with a point:
    POINT_ENV_CREATE     = 0
    POINT_MODBUILD_END   = 1
    POINT_PRESOLVE_START = 2
    POINT_PRESOLVE_END   = 3
    POINT_OPTIMIZE_END   = 4
    POINT_FAILSAFE_START = 10
    POINT_FAILSAFE_END   = 11
    POINT_NAMES = {POINT_ENV_CREATE: 'ENV_CREATE',
                   POINT_MODBUILD_END: 'MODEL_BUILD_END',
                   POINT_PRESOLVE_START: 'PRESOLVE_START',
                   POINT_PRESOLVE_END: 'PRESOLVE_END',
                   POINT_OPTIMIZE_END: 'OPTIMIZE_END',
                   POINT_FAILSAFE_START: 'FAILSAFE_START',
                   POINT_FAILSAFE_END: 'FAILSAFE_END'}
    
    ################################################################
    ### Monitoring #################################################
    ################################################################
    PRINT_HEARTBEAT = 'print_heartbeat'
    PRINT_HEARTBEAT_FREQUENCY = 'print_heartbeat_frequency'
    HEARTBEAT_FREQUENCY = 'Heartbeat_frequency'
    HEARTBEAT_FREQUENCY_DEFAULT = 60

    # notifications are for gurobi notifications
    NOTIFICATION_FREQUENCY = 'notification_frequency'
    NOTIFICATION_FREQUENCY_DEFAULT = 60

    ####################################
    #  Workload Names                  #
    ####################################
    WORKLOAD_DHCP_HHGQ = "DHCP_HHGQ_Workload"
    WORKLOAD_DHCP_HHGQ_DETAILED = "DHCP_HHGQ_Workload_Detailed"
    WORKLOAD_DHCP_PL94_CVAP = "DHCP_PL94_CVAP"
    WORKLOAD_HOUSEHOLD2010 = "HOUSEHOLD2010_WORKLOAD"
    WORKLOAD_DETAILED = "GENERIC_WORKLOAD_DETAILED"

    ####################################
    # Schema names for SchemaMaker     #
    ####################################
    SCHEMA_REDUCED_DHCP_HHGQ = "REDUCED_DHCP_HHGQ_SCHEMA"
    SCHEMA_SF1 = "SF1_SCHEMA"
    SCHEMA_HOUSEHOLD2010 = "HOUSEHOLD2010_SCHEMA"

    ####################################
    #  Attribute Names for SchemaMaker #
    ####################################
    # DHCP
    ATTR_HHGQ = "hhgq"
    ATTR_SEX = "sex"
    ATTR_VOTING = "votingage"
    ATTR_HISP = "hispanic"
    ATTR_CENRACE = "cenrace"
    ATTR_RACE = "race"
    ATTR_AGE = "age"
    ATTR_AGECAT = "agecat"
    ATTR_CITIZEN = "citizen"
    ATTR_REL = "rel"
    # DHCH
    ATTR_HHSEX = "hhsex"
    ATTR_HHAGE = "hhage"
    ATTR_HHHISP = "hisp"
    ATTR_HHRACE = "hhrace"
    ATTR_HHSIZE = "size"
    ATTR_HHTYPE = "hhtype"
    ATTR_HHELDERLY = "elderly"
    ATTR_HHMULTI = "multi"

    ATTR_HHGQ_1940 = "hhgq1940"
    HHGQ_1940_HOUSEHOLD_TOTAL = "household"
    HHGQ_1940_GQLEVELS = "gqlevels"
    HHGQ_1940_INSTLEVELS = "instlevels"
    HHGQ_1940_MENTAL_TOTAL = "gqMentalTotal"
    HHGQ_1940_ELDERLY_TOTAL = "gqElderlyTotal"
    HHGQ_1940_ROOMING_TOTAL = "gqRoomingTotal"

    ATTR_CITIZEN_1940 = "citizen1940"
    ATTR_RACE_1940 = "cenrace1940"
    ATTR_AGE_1940 = "age1940"
    ATTR_SEX_1940 = "sex1940"
    ATTR_HISPANIC_1940 = "hispanic1940"

    ####################################
    # Recode Names for SchemaMaker     #
    ####################################
    # DHCP
    GENERIC_RECODE_TOTAL = "total"
    GENERIC_RECODE_DETAILED = "detailed"

    HISP_TOTAL = "hispTotal"
    HISP_NOT_TOTAL = "notHispTotal"

    CENRACE_NUM = "numraces"
    CENRACE_MAJOR = "majorRaces"
    CENRACE_COMB = "racecomb"
    CENRACE_WHITEALONE = "whiteAlone"
    CENRACE_BLACKALONE = "blackAlone"
    CENRACE_AIANALONE = "aianAlone"
    CENRACE_ASIANALONE = "asianAlone"
    CENRACE_NHOPIALONE = "nhopiAlone"
    CENRACE_SORALONE = "sorAlone"
    CENRACE_TOMR = "tomr"

    HHGQ_HOUSEHOLD_TOTAL = "householdTotal"
    HHGQ_2LEVEL = "hhgq_2level"
    HHGQ_GQLEVELS = "gqlevels"
    HHGQ_INSTLEVELS = "instlevels"
    HHGQ_GQTOTAL = "gqTotal"
    HHGQ_INST_TOTAL = "instTotal"
    HHGQ_CORRECTIONAL_TOTAL = "gqCorrectionalTotal"
    HHGQ_JUVENILE_TOTAL = "gqJuvenileTotal"
    HHGQ_NURSING_TOTAL = "gqNursingTotal"
    HHGQ_OTHERINST_TOTAL  = "gqOtherInstTotal"
    HHGQ_NONINST_TOTAL = "noninstTotal"
    HHGQ_COLLEGE_TOTAL = "gqCollegeTotal"
    HHGQ_MILITARY_TOTAL = "gqMilitaryTotal"
    HHGQ_OTHERNONINST_TOTAL = "gqOtherNoninstTotal"

    AGE_CAT43 = "agecat43"
    AGE_GROUP4 = "agegroup4"
    AGE_GROUP16 = "agegroup16"
    AGE_GROUP64 = "agegroup64"
    AGE_VOTINGLEVELS = "votinglevels"
    AGE_VOTING_TOTAL = "votingTotal"
    AGE_NONVOTING_TOTAL = "nonvotingTotal"
    AGE_UNDER15_TOTAL = "under15total"
    AGE_PREFIX_AGECATS = "prefix_agecats"
    AGE_RANGE_AGECATS = "range_agecats"
    AGE_BINARYSPLIT_AGECATS = "binarysplit_agecats"
    AGE_LT15_TOTAL = "age_lessthan15"
    AGE_GT25_TOTAL = "age_greaterthan25"
    AGE_LT20_TOTAL = "age_lessthan20"
    AGE_LT17GT65_TOTAL = "age_lt17gt65"
    AGE_LT16GT65_TOTAL = "age_lt16gt65"
    AGE_PC03_LEVELS = "agecatPCO3"
    AGE_PC04_LEVELS = "agecatPCO4"
    DAS_1940 = "1940"


    #DHCH

    HHSEX_MALE = "male"
    HHSEX_FEMALE = "female"

    HHRACE_WHITEALONE = "whiteonly"

    HHSIZE_ONE = "size1"
    HHSIZE_TWO = "size2"
    HHSIZE_THREE = "size3"
    HHSIZE_FOUR = "size4"
    HHSIZE_TWOPLUS = "size2plus"
    HHSIZE_NONVACANT_VECT = "sizex0"
    HHSIZE_NOTALONE_VECT = "sizex01"
    HHSIZE_VACANT = "vacant"
    HHSIZE_ALONE_BINARY = "solo"

    HHTYPE_FAMILY = "family"
    HHTYPE_NONFAMILY = "nonfamily"
    HHTYPE_FAMILY_MARRIED = "marriedfamily"
    HHTYPE_FAMILY_MARRIED_WITH_CHILDREN_INDICATOR = "married_with_children_indicator"
    HHTYPE_FAMILY_MARRIED_WITH_CHILDREN_LEVELS = "married_with_children_levels"
    HHTYPE_FAMILY_OTHER = "otherfamily"
    HHTYPE_FAMILY_OTHER_WITH_CHILDREN_INDICATOR = "other_with_children_indicator"
    HHTYPE_FAMILY_OTHER_WITH_CHILDREN_LEVELS = "other_with_children_levels"
    HHTYPE_ALONE = "alone"
    HHTYPE_NOT_ALONE = "invert_alone"
    HHTYPE_NONFAMILY_NOT_ALONE = "notalone"
    HHTYPE_NOT_SIZE_TWO = "non2type"
    HHTYPE_SIZE_TWO_WITH_CHILD = "type2_onechild"
    HHTYPE_SIZE_TWO_COUPLE = "type2_couple"
    HHTYPE_NOT_SIZE_THREE = "non3type"
    HHTYPE_SIZE_THREE_WITH_TWO_CHILDREN = "type3_twochild"
    HHTYPE_SIZE_THREE_NOT_MULTI = "type3_notmulti"
    HHTYPE_SIZE_THREE_COUPLE_WITH_ONE_CHILD = "type3_couple_with_child"
    HHTYPE_SIZE_FOUR_NOT_MULTI = "type4_notmulti"
    HHTYPE_SIZE_FOUR_COUPLE_WITH_TWO_CHILDREN = "type4_couple_with_twochildren"
    HHTYPE_COHABITING = "cohabiting"
    HHTYPE_COHABITING_WITH_CHILDREN_INDICATOR = "cohabiting_with_children_indicator"
    HHTYPE_NO_SPOUSE_OR_PARTNER = "no_spouse_or_partner"
    HHTYPE_NO_SPOUSE_OR_PARTNER_LEVELS = "no_spouse_or_partner_levels"
    HHTYPE_COUPLE_LEVELS = "couplelevels"
    HHTYPE_OPPOSITE_SEX_LEVELS = "oppositelevels"
    HHTYPE_SAME_SEX_LEVELS = "samelevels"
    HHTYPE_NOT_MULTI = "never_multi"
    HHTYPE_OWNCHILD_UNDERSIX = "ownchild_under6"
    HHTYPE_OWNCHILD_UNDER18 = "ownchild_under18"

    

    HHAGE_OVER65 = "hhover65"
    HHAGE_H18 = "hhageH18"
    HHAGE_15TO24 = "age_15to24"
    HHAGE_UNDER60 = "age_under60"
    HHAGE_60TO64 = "age_60to64"
    HHAGE_65TO74 = "age_65to74"
    HHAGE_75PLUS = "age_75plus"
    HHAGE_60PLUS = "age_60plus"


    HHELDERLY_PRESENCE_NOT_60Plus = "pres_under60"
    HHELDERLY_PRESENCE_NOT_65Plus = "pres_under65"
    HHELDERLY_PRESENCE_65TO74 = "elderly3"
    HHELDERLY_PRESENCE_OVER60 = "invert_elderly0"
    HHELDERLY_PRESENCE_NOT_60TO64 = "invert_elderly1"
    HHELDERLY_PRESENCE_NOT_65TO74 = "invert_elderly2"
    HHELDERLY_PRESENCE_NOT_75PLUS = "invert_elderly3"
    HHELDERLY_PRESENCE_60 = "presence60"
    HHELDERLY_PRESENCE_65 = "presence65"
    HHELDERLY_PRESENCE_75 = "presence75"

    HHMULTI_TRUE = "yes_multi"
    HHMULTI_FALSE = "no_multi"

    ################################################################
    ### startup ####################################################
    ################################################################


    ################################################################
    ### dashboard ##################################################
    ################################################################
    MISSION_NAME = 'MISSION_NAME'
    APPLICATIONID='applicationId'
    APPLICATIONID_ENV = 'APPLICATIONID'
    RUN_ID = 'RUN_ID'
    UNICODE_BULLET = "•"


    ################################################################
    ### [Stats]  ###################################################
    ################################################################

    START='START'
    """Property for the stats dictionary. Time an application is started"""
    STOP='STOP'
    """Property for the stats dictionary. Time an application is stoped"""

    TERMINATED='terminated'
    """Property for the stats dictionary. True if an application is terminated stoped"""

    FUNC='func'
    """Property for the stats dictionary. The function in which a stats dictionary was made"""

    STATS_SECTION='stats'
    NOTIFY_GC_MASTER='notify_gc_master'


    ################################################################
    ### [gurobi] ###################################################
    ################################################################
    THREADS_N2N    = 'threads_n2n'     # how many threads to use for national-to-national
    DEFAULT_THREADS_N2N       = 64
    THREADS_GEOLEVEL_PREFIX = "threads_"


#### EXPORTABLE CONSTANTS #####

CC   = _DAS_CONSTANTS()
S    = _SchemaConst() # TODO to be deprecated
DHCP = _DHCPSchemaConst() # TODO to be deprecated
SF1  = _SF1SchemaConst() # TODO to be deprecated

###############################

## Legacy constants


STATS_DIR = "stats_dir"
DAS_S3_LOGFILE_TEMPLATE = "{DAS_S3ROOT}-logs/{CLUSTERID}/DAS/das2020logs/{fname}"
DAS_RUN_PATH = "das/runs/{year}/{month}/DAS-{year}-{month}-{day}T{time}"
OPTIMIZER = 'optimizer'

YMDT_RE                 = re.compile(r'(\d\d\d\d)-(\d\d)-(\d\d)T(\d\d:\d\d:\d\d)')
STATS_URL               = 'https://dasexperimental.ite.ti.census.gov/das/runs/{year}/{month}/{basename}/'
STATS_DIR_DEFAULT       = "$DAS_S3ROOT/rpc/upload"
DEFAULT_GUROBI_STATISTICS_FILENAME = "$DAS_S3ROOT-logs/$CLUSTERID/DAS/runs/$APPLICATIONID/GurobiStatistics"
GUROBI_STATISTICS_FILENAME = "gurobi_statistics_filename"
STATISTICS_PATHNAME_DELIMITER = CC.UNICODE_BULLET
STATISTICS_PARTITIONS = "stats_partitions"
MAX_STATISTICS_PARTITIONS = 10

# Record the stats in the node structure. All of these default to false
# Recording the stats automatically sends them to syslog. We could make
# this an option
# [gurobi]
RECORD_GUROBI_STATS_OPTION = 'record_gurobi_stats'
RECORD_CPU_STATS_OPTION    = 'record_CPU_stats'
RECORD_VM_STATS_OPTION     = 'record_VM_stats'

# Save stats to S3
SAVE_STATS_OPTION          = 'save_stats'

# Print Gurobi stats to stdout
PRINT_GUROBI_STATS_OPTION  = 'print_gurobi_stats'


# oldstyle:

# pylint: disable=bad-whitespace

###########################################
# Analysis Constants
###########################################
RUN_PREFIX = "run_"
RUN_INFEAS = "feas_dict.json"
RUN_DATA = "data/"

###########################################
# Config Constants (Analysis)
###########################################
ANALYSIS = "Analysis"
INPUT_PATH = "input_path"
SUM = "sum"
COLLECT = "collect"
AGGTYPES = "aggtypes"
GEOLEVELS = "geolevels"
QUERIES = "queries"
METRICS = "metrics"
RUN_PREFIX_OPTION = "run_prefix"
RUN_CONTENTS_OPTION = "run_contents"
SCHEMA = "schema"
GROUPS = "groups"
ANALYSIS_GEODICT = "Geodict"
NUMCORES = "numcores"
RECALCULATE_RESULTS = "recalculate_results"
REBUILD_RESMAS = "rebuild_resmas"
ANALYSIS_LOG = "analysis_logfile"

SLDL = "SLDL"
SLDU = "SLDU"


###########################################
# Engine constants
###########################################
NUM_ENGINE_PARTITIONS = "numEnginePartitions"
BUDGET = "budget"
EPSILON_BUDGET_TOTAL = "epsilon_budget_total"
GEOLEVEL_BUDGET_PROP = "geolevel_budget_prop"
QUERIESPROP = "queriesprop"
DETAILEDPROP = "detailedprop"
DPQUERIES = "DPqueries"
SMALLCELLBASEPATH = "small_cell_basepath"
SMALLCELLPROP = "smallcellprop"
SMALLCELLGEOLEVELS = "smallcellgeolevels"
SMALLCELLBASENAME = "smallCellQuery"
POOL_MEASUREMENTS = "pool_measurements"

CHECK_BUDGET = "check_budget"

QUERIESFILE = "queriesfile"
ENGINE = "engine"
DELETERAW = "delete_raw"
MINIMALSCHEMA = "minimalSchema"

BACKUP = "backup"
ROUND_BACKUP = "round_backup"
L2_FEAS_FAIL = "L2 backup/failsafe failed!"
L1_FEAS_FAIL = "L1 rounder (or L1 backup/failsafe) failed!"
PARENT_CONSTRAINTS = "cons_parent"
PARENT_CONSTRAINTS_ROUND = "cons_parent_round"

WORKLOAD = "workload"

BUDGET_DEC_DIGIT_TOLERANCE = 7
SAVENOISY = "save_noisy"
RELOADNOISY = "reload_noisy"
SAVED_NOISY_APP_ID = "saved_noisy_app_id"
NOISY_MEASUREMENTS_POSTFIX = "noisy_measurements_postfix"
APPID_NO_SPARK = "APPID_No_Spark"


###########################################
# HDMM Constants
###########################################
STRATEGY_TYPE = "strategy_type"
HDMM = "hdmm"
PIDENTITY = "pidentity"
MARGINAL = "marginal"
PS_PARAMETERS = "ps_parameters"
###########################################
# Reader Constants
###########################################
READER = "reader"
PATH = "path"
TABLES = "tables"
TABLE_CLASS = "class"
VARS = "variables"
RECODE_VARS = "recode_variables"
PRE_RECODER = "recoder"
LEGAL = "legal"
VAR_TYPE = "type"
NEWRECODER = "newrecoder"
HEADER = "header"
DELIMITER = "delimiter"
CSV_COMMENT = "comment"
LINKAGE = "linkage"
CTABLES = "constraint_tables"
PTABLE = "privacy_table"
CONSTRAINTS = "constraints"
THECONSTRAINTS = "theConstraints"
INVARIANTS = "Invariants"
THEINVARIANTS = "theInvariants"
GEOGRAPHY = "geography"
HISTOGRAM = "histogram"
MEASURE_RDD_TIMES = "measure_rdd_times"
UNIQUE = "unique"
PICKLEDPATH = "pickled.path"


# spar_table partitioner
NUM_READER_PARTITIONS = "numReaderPartitions"
RANGE_PARTITION = "rangePartition"
READER_PARTITION_LEN = "readerPartitionLen"
# table_reader partitioner
PARTITION_BY_BLOCK_GROUP = "partition_by_block_group"

VALIDATE_INPUT_DATA_CONSTRAINTS = "validate_input_data_constraints"

###########################################
# Gurobi Constants
###########################################

GUROBI = 'gurobi'
GUROBI_PATH = 'gurobi_path'
GUROBI_SECTION = "gurobi"
GRB_LICENSE_FILE = 'GRB_LICENSE_FILE'
GUROBI_LIC = "gurobi_lic"
GUROBI_LOGFILE_NAME = 'gurobi_logfile_name'
OUTPUT_FLAG = 'OutputFlag'
OPTIMALITY_TOL = 'OptimalityTol'
BAR_CONV_TOL = 'BarConvTol'
BAR_QCP_CONV_TOL = 'BarQCPConvTol'
BAR_ITER_LIMIT = 'BarIterLimit'
FEASIBILITY_TOL = 'FeasibilityTol'
THREADS = 'Threads'
PRESOLVE = 'Presolve'
PYTHON_PRESOLVE = "python_presolve"
NUMERIC_FOCUS = 'NumericFocus'
ENVIRONMENT = 'ENVIRONMENT'
SYSLOG_UDP = 514
CLUSTER_OPTION = 'cluster'
CENSUS_CLUSTER = 'Census'
GAM_CLUSTER = 'GAM'
GRB_APP_NAME = "GRB_APP_NAME"
GRB_ISV_NAME = "GRB_ISV_NAME"
GRB_ENV3 = "GRB_Env3"
GRB_ENV4 = "GRB_Env4"
TIME_LIMIT = "TimeLimit"
SAVE_LP_PATH = "save_lp_path"
SAVE_LP_PATTERN = 'save_lp_pattern'
GUROBI_LICENSE_MAX_RETRIES = 10
GUROBI_LICENSE_RETRY_EXPONENTIAL_BASE = 1.3e-3
GUROBI_LICENSE_RETRY_JITTER           = 2.000


################################
# Gurobi Performance Statistics System
################################

###########################################
# Spark Constants
###########################################

SPARK = "spark"
NAME = "name"
MASTER = "master"
LOGLEVEL = "loglevel"
ZIPFILE = "ZIPFILE"
MAX_STATS_PARTS = 8              # number of parts for saving statistics
SPARK_LOCAL_DIR = 'spark.local.dir'
SPARK_EVENTLOG_DIR = 'spark.eventLog.dir'

###########################################
# String, Regex, and I/O Constants
###########################################
EMPTY = ""
S3_PREFIX = "s3://"
HDFS_PREFIX = "hdfs://"

# regex for removing whitespace characters and splitting by commas
# use with the re module
# re.split(DELIM, string_to_split)
# "   apple  , hat, fork,spoon,    pineapple" => ['apple', 'hat', 'fork', 'spoon', 'pineapple']
REGEX_CONFIG_DELIM = r"^\s+|\s*,\s*|\s+$"

###########################################
# Config Constants (DAS)
###########################################
CONFIG_INI = "config.ini"
GEODICT = "geodict"
GEODICT_GEOLEVELS = "geolevel_names"
GEODICT_LENGTHS = "geolevel_leng"
NATIONAL_GEOCODE = ""
ROOT_GEOCODE = '0'              # was previously hard-coded in nodes.py:277
PRE_RELEASE = 'pre_release'
POSTPROCESS_ONLY = "postprocess_only"
RELEASE_SUFFIX = '.release.zip'

################################################################
## From nodes.py
################################################################

RAW = "raw"
RAW_HOUSING = "raw_housing"
SYN = "syn"
CONS = "cons"
INVAR = "invar"
GEOCODEDICT = "geocodeDict"
GEOCODE = "geocode"
GEOLEVEL = "geolevel"
DP_QUERIES = "dp_queries"
PARENTGEOCODE = "parentGeocode"
DETAILED = "detailed"  # Detailed DPQuery name. Also a config value for it.


###########################################
# Writer Config Constants
###########################################
ALL = "ALL"
WRITER = "writer"
WRITER_SECTION = 'writer'
OVERWRITE_FLAG = "overwrite_flag"
DEFAULT_CLASSIFICATION_LEVEL = "Classification level not specified in config file"
CLASSIFICATION_LEVEL = "classification_level"
SPLIT_BY_STATE = "split_by_state"
STATE_CODES = "state_codes"
FEAS_DICT_JSON = "feas_dict.json"
OUTPUT_FNAME = "output_fname"   # deprecated
OUTPUT_PATH = 'output_path'
OUTPUT_DATAFILE_NAME = 'output_datafile_name'
WRITE_METADATA = 'write_metadata'
WRITE_METADATA_DEFAULT = True
KEEP_ATTRS = "keep_attrs"
PICKLE_BATCH_SIZE = "pickle_batch_size"
PRODUCE = "produce_flag"
NUM_PARTS = "num_parts"
NUM_PARTS_DEFAULT = 100
WRITE_TYPE = "write_type"
MINIMIZE_NODES = "minimize_nodes"
JSON = "json"
WRITE_JSON_KEEP_ATTRS = "json_keepAttrs"
BLANK = ""
GIT = "git"
SHOW = "show"
UTF_8 = "utf-8"
GIT_COMMIT = "git-commit"
SAVE_GIT_COMMIT = "save_git_commit"
S3CAT = 's3cat'
S3CAT_DEFAULT = True
S3CAT_VERBOSE = 's3cat_verbose'
S3CAT_SUFFIX = 's3cat_suffix'
MULTIWRITER_WRITERS = "multiwriter_writers"
UPLOAD_LOGFILE = 'upload_logfile'

###########################################
# Schema name constants / Datadict constants
###########################################
# query crosses should be denoted by separating variables with an asterisk padded by whitespace
# Example: "hhgq * sex * age"
SCHEMA_CROSS_SPLIT_DELIM = r"^\s+|\s*\*\s*|\s+$"
SCHEMA_CROSS_JOIN_DELIM = " * "

DAS_PL94 = "PL94"
DAS_PL94_CVAP = "PL94_CVAP"
DAS_PL94_P12 = "PL94_P12"
DAS_1940 = "1940"
DAS_SF1 = "SF1_Person"
DAS_DHCP_HHGQ = "DHCP_HHGQ"
DAS_Household2010 = "Household2010"
DAS_TenUnit2010 = "TenUnit2010"

###########################################
# Schema histogram variables names
###########################################

# TODO: move all references to S (from constants.py, imported from otherconsts.schemaconst)

HHGQ = "hhgq"
SEX = "sex"
VOTING = "votingage"
HISP = "hispanic"
CENRACE = "cenrace"
RACE = "race"
AGE = "age"
AGECAT = "agecat"
CITIZEN = "citizen"
REL = "rel"


###########################################
# HDMM closed-form error engine constants
###########################################
HDMM_ERROR_MODE = "errorMode"
HDMM_ERROR_TYPE = "errorType"
HDMM_ERROR = "hdmmError"

TENABILITY_OUTPUT_ROOT = "tenabilityOutputRoot"

ERROR_GEOLEVELS = "errorGeolevels"
ERROR_EPSILONS = "errorEpsilons"

###########################################
# Miscellaneous Constants
###########################################


###########################################
# Geographic Level Constants (crosswalk.py and Analysis)
###########################################
NATION = 'NATION'
STATE = 'STATE'
COUNTY = 'COUNTY'
TRACT_GROUP = 'TRACT_GROUP'
TRACT = 'TRACT'
BLOCK_GROUP = 'BLOCK_GROUP'
BLOCK = 'BLOCK'

CD = 'CD'           # Congressional District (111th)
SLDU = 'SLDU'       # State Legislative District (Upper Chamber) (Year 1)
SLDL = 'SLDL'       # State Legislative District (Lower Chamber) (Year 1)
VTD = 'VTD'         # Voting District
COUSUB = 'COUSUB'   # County Subdivision (FIPS)
SUBMCD = 'SUBMCD'   # Subminor Civil Division (FIPS)
UA = 'UA'           # Urban Areas
CBSA = 'CBSA'       # Metropolitan Statistical Area
METDIV = 'METDIV'   # Metropolitan Division
CSA = 'CSA'         # Combined Statistical Area
UGA = 'UGA'         # Urban Growth Area
PUMA = 'PUMA'       # Public Use Microdata Area
PLACE = 'PLACE'     # Place (FIPS)
ZCTA5 = 'ZCTA5'     # ZIP Code Tabulation Area (5-digit)
