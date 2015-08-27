#!/bin/bash

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

##################
### Camus2Hive ###
##################

# Dependencies validation
command -v hive >/dev/null 2>&1 || {
	echo "The hive command must be defined. Aborting."
	exit 1
}

command -v hdfs >/dev/null 2>&1 || {
	echo "The hdfs command must be defined. Aborting."
	exit 1
}

function print_usage() {
    echo "Usage: `basename $0` <camus_destination_dir> [-d database] [-r repository_uri]"
    echo ""
    echo "camus_destination_dir"
    echo "      HDFS path where Camus stores its destination directory. (required)"
    echo ""
    echo "-d,--database <database>"
    echo "      name of database to use, default is 'default'. (required)"
    echo ""
    echo "-p,--partition-scheme <(daily|hourly)>"
    echo "      the partitioning granularity of the input data. (required)"
    echo ""
    echo "-r,--repository <repository_uri>"
    echo "      http uri to the schema repository. (required)"
    echo ""
    echo "-h,--help"
    echo "      prints this message"
    echo ""
    exit 1
}

# Process the arguments

# Remove trailing slashes (if the supplied path is just / then $CAMUS_DESTINATION_DIR will be empty, but that's ok since commands below always add a slash after anyway...)
CAMUS_DESTINATION_DIR=`echo $1 | sed -e 's%\(/\)*$%%g'`

if [[ -z "$CAMUS_DESTINATION_DIR" ]]; then
    print_usage
    exit 1
fi
shift
while [[ $# -gt 0 ]]; do
    opt="$1"
    shift
    current_arg="$1"
    case "$opt" in
    "-d"|"--database")
        DATABASE=$current_arg
        shift
        ;;
    "-r"|"--repository")
        AVRO_SCHEMA_REPOSITORY=$current_arg
        shift
        ;;
    "-p"|"--partition-scheme")
        PARTITION_SCHEME=$current_arg
        shift
        if [ "${PARTITION_SCHEME}" != "daily" ] && [ "${PARTITION_SCHEME}" != "hourly" ]; then
            echo "ERROR: Invalid value for --partition-scheme"
            print_usage
            exit 1
        fi
        ;;
    "-h"|"--help")
        print_usage
        exit 0
        ;;
    *)
        echo "Invalid argument $opt"
        print_usage
        exit 1
        ;;
    esac
done
if [[ -z "$DATABASE" ]]; then
    DATABASE="default"
fi
HIVE="hive --database $DATABASE -S"

# What namenode Hive is communicating with for this database
NAME_NODE_URI=$(${HIVE} -e "describe database $DATABASE;" | egrep -v '^\[HiveConf' | sed -re 's%.*\t(hdfs://[a-zA-Z0-9]+)(:[0-9]+)?.*%\1\2%')

# Behavior config
REQUERY_HADOOP_DIRS=true
EXIT_ON_ERROR=false
PRINT_HIVE_STDERR=false

# This directory and file hold state for the whole job
WORK_DIR='temp_camus2hive'
TOPIC_NAMES="$WORK_DIR/topic_names"

# These files hold state per table/topic (and are zero-ed out between each)
EXISTING_HIVE_PARTITIONS_WITH_SLASHES="$WORK_DIR/hive_partitions_with_slashes"
EXISTING_HIVE_PARTITIONS="$WORK_DIR/hive_partitions"
EXISTING_CAMUS_PARTITIONS="$WORK_DIR/camus_partitions"
HIVE_PARTITIONS_TO_ADD="$WORK_DIR/hive_partitions_to_add"
HIVE_ADD_PARTITION_STATEMENTS="$WORK_DIR/hive_add_partitions_statements"
HIVE_STDERR="$WORK_DIR/hive_stderr"

# Return 0 if everything is ok
function hive_success_check {
    local status=$?
	MESSAGE=$1
	if [[ $status -ne 0 ]]; then
		if [[ -z $MESSAGE ]]; then
			echo "HIVE ERROR :'((( ..."
		else
			echo "HIVE ERROR: $MESSAGE"
		fi

		if $PRINT_HIVE_STDERR ; then cat $HIVE_STDERR; fi

		if $EXIT_ON_ERROR ; then exit 1; fi

		return 1
	else
		return 0
	fi
}

function latest_schema_for_topic {
    # Need to strip prefix, convert class underscores to dots and put back together for querying repo
    local prefix=$(echo $1 | cut -d_ -f1)
    local class=$(echo $1 | cut -d_ -f2- | sed s/_/./g)
    local topic="${prefix}_${class}"
    local uri="$AVRO_SCHEMA_REPOSITORY/$topic/latest"
    # This gets returned in the format ID\tSCHEMA
    local latest=$(curl -fs ${uri})
    if [[ -z $latest ]]; then
        # We need to crap out here because if this fails, we could lose data
        echo "Could not access avro repository at $uri"
        exit 1
    fi
    local latest_id=$(echo $latest | awk '{print $1}')
    local latest_schema=$(echo $latest | awk '{$1=""; print substr($0,2)}')

    eval "$2='$latest_id'"
    eval "$3='$latest_schema'"
}

## create_hive_table ${topic_table} "${PARTITION_BY}" ${AVRO_SCHEMA_URL}
function create_hive_table {
    topic_table=$1
    PARTITION_BY=$2
    AVRO_SCHEMA_URL=$3

    ${HIVE} -e "\
    CREATE EXTERNAL TABLE ${topic_table} \
      PARTITIONED BY ${PARTITION_BY} \
      ROW FORMAT SERDE \
        'org.apache.hadoop.hive.serde2.avro.AvroSerDe' \
      STORED AS INPUTFORMAT \
        'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' \
      OUTPUTFORMAT \
        'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' \
      TBLPROPERTIES ( \
        'avro.schema.url'='${NAME_NODE_URI}${LATEST_SCHEMA}' \
      );" > /dev/null 2> $HIVE_STDERR
}


#####################################
#        Let's get to work
#####################################

mkdir -p $WORK_DIR

if $REQUERY_HADOOP_DIRS ; then
	hdfs dfs -ls $CAMUS_DESTINATION_DIR/ | grep -v 'Found .* items' | sed s%.*$CAMUS_DESTINATION_DIR/%% > $TOPIC_NAMES
fi

while read topic; do
    ## Strip/convert some things in the topic names to make them nicer in hive...
    # (Hive can't handle "-" in table name so translate it)
    topic_table=$(echo $topic | sed s/-/_/g | sed -r "s/^\(prd|prod|stg|dev\)//" | sed "s/^_vsw_avrodto_//")

    # append the partitioning scheme (i.e. daily/hourly) to the table name
    topic_table="${topic_table}_${PARTITION_SCHEME}"

    # define the table partitioning scheme
    if [ "${PARTITION_SCHEME}" == "daily" ]; then
        PARTITION_BY="(pyear int, pmonth int, pday int)"
    elif [ "${PARTITION_SCHEME}" == "hourly" ]; then
        PARTITION_BY="(pyear int, pmonth int, pday int, phour int)"
    fi

	# Zero-out the per-topic state files (probably not necessary but whatever...)
	> $EXISTING_HIVE_PARTITIONS_WITH_SLASHES
	> $EXISTING_HIVE_PARTITIONS
	> $EXISTING_CAMUS_PARTITIONS
	> $HIVE_PARTITIONS_TO_ADD
	> $HIVE_ADD_PARTITION_STATEMENTS
	> $HIVE_STDERR

    if [[ ! -z "$AVRO_SCHEMA_REPOSITORY" ]]; then
        latest_schema_for_topic $topic SCHEMA_ID SCHEMA_TEXT
        echo $SCHEMA_TEXT > $WORK_DIR/$SCHEMA_ID
        SCHEMA_DIR=$CAMUS_DESTINATION_DIR/$topic/schemas
        LATEST_SCHEMA=$SCHEMA_DIR/$SCHEMA_ID
        
        hdfs dfs -mkdir -p $SCHEMA_DIR

        hdfs dfs -test -e $LATEST_SCHEMA
        if [[  $? -ne 0 ]]; then
            hdfs dfs -put $WORK_DIR/$SCHEMA_ID $LATEST_SCHEMA
        else
            echo "Schema '$SCHEMA_ID' already exists for topic '${topic}'"
        fi
    fi

	# Check if the table already exists in Hive
    ${HIVE} -e "SHOW PARTITIONS $topic_table" 1> $EXISTING_HIVE_PARTITIONS_WITH_SLASHES 2> $HIVE_STDERR

    if ! hive_success_check "Table '$topic_table' does not currently exist in Hive (or Hive returned some other error on SHOW PARTITIONS $topic_table)."; then
        if [[ ! -z "$AVRO_SCHEMA_REPOSITORY" ]]; then
            # Create the table from the latest schema, this assumes a Validator already made sure it is safe to do so
            echo "Creating table ${topic_table} from ${LATEST_SCHEMA}"

            create_hive_table "${topic_table}" "${PARTITION_BY}" "${NAME_NODE_URI}${LATEST_SCHEMA}"

            if hive_success_check "Some errors occurred while creating the table '$topic_table'"; then
                echo "Successfully created Hive table '$topic_table' from schema $SCHEMA_ID :D !"
            fi
        fi
    else
        if [[ ! -z "$AVRO_SCHEMA_REPOSITORY" ]]; then
            # Update the hive table to the latest schema, which may not have been updated, but oh well
            ${HIVE} -e "\
            ALTER TABLE ${topic_table} \
            SET TBLPROPERTIES ( \
                'avro.schema.url'='${NAME_NODE_URI}${LATEST_SCHEMA}' \
                \
                );" > /dev/null 2> $HIVE_STDERR

            if hive_success_check "Some errors occurred while updating schema for the table '$topic_table'"; then
                echo "Successfully updated Hive table '$topic_table' to schema $SCHEMA_ID :D !"
            fi
        fi
    fi

    cat $EXISTING_HIVE_PARTITIONS_WITH_SLASHES | sed 's%/%, %g' > $EXISTING_HIVE_PARTITIONS

    # Extract all partitions currently ingested by Camus
    if [ "${PARTITION_SCHEME}" == "hourly" ]; then
        hdfs dfs -ls -R $CAMUS_DESTINATION_DIR/$topic | sed "s%.*$CAMUS_DESTINATION_DIR/$topic/hourly/\([0-9]*\)/\([0-9]*\)/\([0-9]*\)/\([0-9]*\)/.*%pyear=\1, pmonth=\2, pday=\3, phour=\4%" | grep "year.*" | sort | uniq > $EXISTING_CAMUS_PARTITIONS
    elif [ "${PARTITION_SCHEME}" == "daily" ]; then
        hdfs dfs -ls -R $CAMUS_DESTINATION_DIR/$topic | sed "s%.*$CAMUS_DESTINATION_DIR/$topic/daily/\([0-9]*\)/\([0-9]*\)/\([0-9]*\)/.*%pyear=\1, pmonth=\2, pday=\3%" | grep "year.*" | sort | uniq > $EXISTING_CAMUS_PARTITIONS
    fi

    # find all new partitions
    grep -v -f $EXISTING_HIVE_PARTITIONS $EXISTING_CAMUS_PARTITIONS > $HIVE_PARTITIONS_TO_ADD

    echo "$topic currently has $(cat $EXISTING_CAMUS_PARTITIONS | wc -l) partitions in Camus directories, $(cat $EXISTING_HIVE_PARTITIONS | wc -l) in Hive and thus $(cat $HIVE_PARTITIONS_TO_ADD | wc -l) left to add to Hive"

    if [ "${PARTITION_SCHEME}" == "hourly" ]; then
        sed "s%\(pyear=\([0-9]*\), pmonth=\([0-9]*\), pday=\([0-9]*\), phour=\([0-9]*\)\)%ALTER TABLE $topic_table ADD IF NOT EXISTS PARTITION (\1) LOCATION '$CAMUS_DESTINATION_DIR/$topic/hourly/\2/\3/\4/\5';%" < $HIVE_PARTITIONS_TO_ADD > $HIVE_ADD_PARTITION_STATEMENTS
    elif [ "${PARTITION_SCHEME}" == "daily" ]; then
        sed "s%\(pyear=\([0-9]*\), pmonth=\([0-9]*\), pday=\([0-9]*\)\)%ALTER TABLE $topic_table ADD IF NOT EXISTS PARTITION (\1) LOCATION '$CAMUS_DESTINATION_DIR/$topic/daily/\2/\3/\4';%" < $HIVE_PARTITIONS_TO_ADD > $HIVE_ADD_PARTITION_STATEMENTS
    fi

    ${HIVE} -f $HIVE_ADD_PARTITION_STATEMENTS > /dev/null 2> $HIVE_STDERR
		
    if hive_success_check "Some errors occurred while adding partitions to table '$topic_table'" && [[ -s $HIVE_PARTITIONS_TO_ADD ]]; then
        echo "$(cat $HIVE_PARTITIONS_TO_ADD | wc -l) partitions successfully added to Hive table '$topic_table' :D !"
    fi
	echo ""
done < $TOPIC_NAMES

echo "Finished processing $(cat $TOPIC_NAMES | wc -l) topic(s) :)"
