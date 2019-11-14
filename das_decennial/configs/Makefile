# Set variables that will be used in testing

validate_all_configs:
	echo Recursively scan all config files and make sure all include files can be resolved
	DAS_S3ROOT="FOO" JBID="FOO" python validate_configs.py `find . -name '*.ini' -print | grep -v lecle301`


