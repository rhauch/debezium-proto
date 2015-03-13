#!/bin/bash

# Any ENV variable that begins with 'DEBEZIUM_' will be used to set or override a corresponding property in 
# the service configuration. The rules for converting an ENV variable into a property are as follows:
#
# 1. Any variable that does not start with 'DEBEZIUM_' is ignored.
# 2. The variable whose name is equal to "DEBEZIUM_" (with no other characters) is ignored.
# 3. The variable whose name is equal to "DEBEZIUM_HOME" is ignored.
# 4. The property name is converted from the variable name after:
#    a. The "DEBEZIUM_" prefix is removed;
#    b. All characters are lower-cased; and
#    c. All '_' characters in the variable name are converted to '.' characters.
# 
# For example, the "DEBEZIUM_JOB_NAME" ENV variable is converted to the "job.name" property.
#
# The value of the ENV variable may not contain a '@' character.
# 
# If the property name exists in the service's properties file, the value is replaced with the ENV variable's value.
# Otherwise, the property (name and value) are appended to the service's properties file.
#

if [[ -n "$ZOOKEEPER" ]]; then
  export DEBEZIUM_SYSTEMS_DEBEZIUM_CONSUMER_ZOOKEEPER_CONNECT=$ZOOKEEPER
fi

if [[ -n "$KAFKA" ]]; then
  export DEBEZIUM_SYSTEMS_DEBEZIUM_PRODUCER_METADATA_BROKER_LIST=$KAFKA
fi

if [[ -n "$BATCH_SIZE" ]]; then
  export DEBEZIUM_SYSTEMS_DEBEZIUM_PRODUCER_BATCH_NUM_MESSAGES=$BATCH_SIZE
fi

if [[ -n "$CHECKPOINT_INTERVAL" ]]; then
  export DEBEZIUM_TASK_COMMIT_MS=$(echo $(($CHECKPOINT_INTERVAL * 1000)) )
fi

if [[ -n "$CHECKPOINT_REPLICATION" ]]; then
  export DEBEZIUM_TASK_CHECKPOINT_REPLICATION_FACTOR=$CHECKPOINT_REPLICATION
fi

if [[ -n "$JOB_ID" ]]; then
  export DEBEZIUM_TASK_JOB_ID=$JOB_ID
else
  export JOB_ID=$(sed -n 's/^job.id*=*//p' $DEBEZIUM_HOME/config/service.properties)
fi

if [[ -z "$SAMZA_CONTAINER_NAME" ]]; then
  export SAMZA_CONTAINER_NAME=$(sed -n 's/^job.name*=*//p' $DEBEZIUM_HOME/config/service.properties)
fi

if [[ -z "$LOG_CONFIG" ]]; then
  export LOG_CONFIG=$DEBEZIUM_HOME/config/log4j.xml
fi

if [[ -n "$ZOOKEEPER" ]]; then
  export DEBEZIUM_SYSTEMS_DEBEZIUM_CONSUMER_ZOOKEEPER_CONNECT=$ZOOKEEPER
fi

if [[ -n "$KAFKA" ]]; then
  export DEBEZIUM_SYSTEMS_DEBEZIUM_PRODUCER_METADATA_BROKER_LIST=$KAFKA
fi


for VAR in `env`
do
  if [[ $VAR =~ ^DEBEZIUM_ && ! $VAR =~ ^DEBEZIUM_HOME ]]; then
    prop_name=`echo "$VAR" | sed -r "s/DEBEZIUM_(.*)=.*/\1/g" | tr '[:upper:]' '[:lower:]' | tr _ .`
    env_var=`echo "$VAR" | sed -r "s/(.*)=.*/\1/g"`
    if egrep -q "(^|^#)$prop_name=" $DEBEZIUM_HOME/config/service.properties; then
        #note that no config values may contain an '@' char
        sed -r -i "s@(^|^#)($prop_name)=(.*)@\2=${!env_var}@g" $DEBEZIUM_HOME/config/service.properties 
    else
        echo "$prop_name=${!env_var}" >> $DEBEZIUM_HOME/config/service.properties
    fi
  fi
done

# Start the service ...

export JAVA_OPTS="-Xloggc:$DEBEZIUM_HOME/logs/gc.log -Dlog4j.configuration=file:$LOG_CONFIG -Dsamza.log.dir=$DEBEZIUM_HOME/logs -Dsamza.container.name=$SAMZA_CONTAINER_NAME"

$DEBEZIUM_HOME/bin/run-job.sh \
  --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory \
  --config-path=file:$DEBEZIUM_HOME/config/service.properties \
  