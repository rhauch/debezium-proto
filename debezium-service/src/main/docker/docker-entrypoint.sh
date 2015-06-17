#!/bin/bash

# Exit immediately if a *pipeline* returns a non-zero status. (Add -x for command tracing)
set -e

usage() {
    echo "The following command is used to start the Debezium service in a new Docker container:"
    echo ""
    echo "   start"
    echo ""
    echo "This is the default command that is used when you don't provide a command. The following"
    echo "command will display the help for this image:"
    echo ""
    echo "    help"
    echo ""
    echo "The container can also run arbitrary commands. For example, to obtain a bash shell in a new"
    echo "container or in an already-running container:"
    echo ""
    echo "    bash"
    echo ""
    echo ""
    echo "Environment variables"
    echo "---------------------"
    echo ""
    echo "You can pass multiple environment variables to alter the Kafka configuration:"
    echo ""
    echo "   KAFKA                         Recommended. Set this to the list of addresses for the Kafka brokers."
    echo "                                 This list is simply used to establish the proper connections to the"
    echo "                                 leaders for the required partitions. If this container is started with"
    echo "                                 a link to another container running Kafka on port 9092, then this environment"
    echo "                                 variable need not be set since it can be determined automatically."
    echo "                                 Otherwise, it should be set with an explicit value."
    echo "   ZOOKEEPER                     Recommended. Set this to the list of addresses for the Zookeeper service."
    echo "                                 If this container is started with a link to another container running"
    echo "                                 Zookeeper on port 2181, then this environment variable need not be set"
    echo "                                 since it can be determined automatically. Otherwise, it should be set"
    echo "                                 with an explicit value."
    echo "   JOB_ID                        Recommended. Set this to a unique number for each instance of the Debezium"
    echo "                                 service running in the cluster. The job ID is used within the checkpoints"
    echo "                                 for failure recovery. The default is '1', which is fine if only one instance"
    echo "                                 of this service is run; otherwise it should be explicitly set."
    echo "   CHECKPOINT_INTERVAL_SECONDS   Recommended. Set this to the time between checkpoint commits in seconds."
    echo "                                 The default is to checkpoint every 60 seconds. The frequency of checkpointing"
    echo "                                 affects failure recovery: if a container fails unexpectedly (e.g. due to"
    echo "                                 crash or machine failure) and is restarted, it resumes processing at the"
    echo "                                 last checkpoint. Any messages processed since the last checkpoint on the"
    echo "                                 failed container are processed again. Checkpointing more frequently reduces"
    echo "                                 the number of messages that may be processed twice, but also uses more resources."
    echo "   CHECKPOINT_REPLICATION        Recommended. Set this to the number of Kafka nodes to which you want the "
    ehco "                                 checkpoint topic replicated for durability. The default is '1'."
    echo "   LOG_LEVEL                     Optional. Set the level of detail for Zookeeper's application log"
    echo "                                 written to STDOUT and STDERR. Valid values are 'INFO' (default), 'WARN',"
    echo "                                 'ERROR', 'DEBUG', or 'TRACE'."
    echo ""
    echo "Environment variables that start with 'DEBEZIUM_' will apply to the service configuration file."
    echo "Each environment variable name to a configuration variable name by:"
    echo ""
    echo "  1. removing the 'DEBEZIUM_' prefix;"
    echo "  2. lowercasing all characters; and"
    echo "  3. converting all '_' characters to '.' characters"
    echo ""                  
    echo "For example, the environment variable 'DEBEZIUM_JOB_NAME' is converted to the 'job.name' property."
    echo "If the property name exists in the service's properties file, the value is replaced with the environment"
    echo "variable's value. Otherwise, the property (name and value) are appended to the service's properties file."
    echo ""
    echo "The value of the environment variable may not contain a '@' character."
    echo ""
    echo ""
    echo "Volumes"
    echo "-------"
    echo ""
    echo "The container exposes two volumes:"
    echo ""
    echo "  /debezium/data     Directory in which the service can write persistent or cached data that"
    echo "                     may be reused by the service if/when it is restarted."
    echo ""
    echo "  /debezium/logs     Debezium writes its log files within this directory."
    echo ""
    echo ""
}

if [[ -z "$ZOOKEEPER" ]]; then
    # Look for any environment variables set by Docker container linking. For example, if the container
    # running Zookeeper were named 'zoo' in this container, then Docker should have created several envs,
    # such as 'ZOO_PORT_2181_TCP'. If so, then use that to automatically set the 'ZOOKEEPER' env varg.
    export ZOOKEEPER=$(env | grep .*PORT_2181_TCP= | sed -e 's|.*tcp://||' | paste -sd ,)
fi
if [[ "x$ZOOKEEPER" = "x" ]]; then
    echo "The ZOOKEEPER variable must be set, or the container must be linked to one that runs Zookeeper."
    exit 1
else
    echo "Using ZOOKEEPER=$ZOOKEEPER"
fi
if [[ -n "$ZOOKEEPER" ]]; then
  export DEBEZIUM_SYSTEMS_KAFKA_CONSUMER_ZOOKEEPER_CONNECT=$ZOOKEEPER
  unset ZOOKEEPER
fi

if [[ -z "$KAFKA" ]]; then
    # Look for any environment variables set by Docker container linking. For example, if the container
    # running Kafka were named 'kafka' in this container, then Docker should have created several envs,
    # such as 'KAFKA_PORT_9092_TCP'. If so, then use that to automatically set the 'KAFKA' env var.
    export KAFKA=$(env | grep .*PORT_9092_TCP= | sed -e 's|.*tcp://||' | uniq | paste -sd ,)
fi
if [[ "x$KAFKA" = "x" ]]; then
    echo "The KAFKA variable must be set, or the container must be linked to one that runs Kafka."
    #exit 1
else
    echo "Using KAFKA=$KAFKA"
fi
if [[ -n "$KAFKA" ]]; then
  export DEBEZIUM_SYSTEMS_KAFKA_PRODUCER_BOOTSTRAP_SERVERS=$KAFKA
  unset KAFKA
fi

if [[ -n "$CHECKPOINT_INTERVAL_SECONDS" ]]; then
  export DEBEZIUM_TASK_COMMIT_MS=$(echo $(($CHECKPOINT_INTERVAL_SECONDS * 1000)) )
fi

if [[ -n "$CHECKPOINT_REPLICATION" ]]; then
  export DEBEZIUM_TASK_CHECKPOINT_REPLICATION_FACTOR=$CHECKPOINT_REPLICATION
else
  export DEBEZIUM_TASK_CHECKPOINT_REPLICATION_FACTOR=1
fi

if [[ -n "$JOB_ID" ]]; then
  export DEBEZIUM_TASK_JOB_ID=$JOB_ID
else
  export JOB_ID=$(sed -n 's/^job.id*=*//p' $DEBEZIUM_HOME/config/service.properties)
fi

if [[ -z "$CONTAINER_NAME" ]]; then
  export CONTAINER_NAME=$(sed -n 's/^job.name*=*//p' $DEBEZIUM_HOME/config/service.properties)
fi

if [[ -z "$LOG_CONFIG" ]]; then
  export LOG_CONFIG=$DEBEZIUM_HOME/config/log4j.properties
fi

if [[ -z "$LOG_DIR" ]]; then
  export LOG_DIR=$DEBEZIUM_HOME/logs
fi

# Process the argument to this container ...
case $1 in
    start)
        #
        # Process the logging-related environment variables.
        #
        if [[ -z "$LOG_LEVEL" ]]; then
            LOG_LEVEL="INFO"
        fi
        ROOT_LOGGER="$LOG_LEVEL,console"
        #
        # Process all environment variables that start with 'DEBEZIUM_' 
        # (but not 'DEBEZIUM_HOME', 'DEBEZIUM_VERSION', or 'DEBEZIUM_USER'):
        #
        for VAR in `env`
        do
          env_var=`echo "$VAR" | sed -r "s/(.*)=.*/\1/g"`
          if [[ $env_var =~ ^DEBEZIUM_ && $env_var != "DEBEZIUM_HOME" && $env_var != "DEBEZIUM_VERSION" && $env_var != "DEBEZIUM_USER" ]]; then
            prop_name=`echo "$VAR" | sed -r "s/DEBEZIUM_(.*)=.*/\1/g" | tr '[:upper:]' '[:lower:]' | tr _ .`
            if egrep -q "(^|^#)$prop_name=" $DEBEZIUM_HOME/config/service.properties; then
                #note that no config values may contain an '@' char
                sed -r -i "s@(^|^#)($prop_name)=(.*)@\2=${!env_var}@g" $DEBEZIUM_HOME/config/service.properties 
            else
                echo "$prop_name=${!env_var}" >> $DEBEZIUM_HOME/config/service.properties
            fi
          fi
        done
        #
        # Set up the Java options ...
        #
        export JAVA_OPTS="-Xloggc:$LOG_DIR/gc.log "`
                        `"-Dlog4j.configuration=file:$LOG_CONFIG "`
                        `"-Ddebezium.logs.dir=$LOG_DIR "`
                        `"-Ddebezium.root.logger=${ROOT_LOGGER} "`
                        `"-Ddebezium.console.threshold=$LOG_LEVEL "`
                        `"-Dsamza.container.name=$CONTAINER_NAME"
        #
        # Start the service ...        
        #
        exec $DEBEZIUM_HOME/bin/run-job.sh \
             --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory \
             --config-path=file:$DEBEZIUM_HOME/config/service.properties
        ;;
    -h|--h|--help|help)
        usage; exit 1
        ;;
esac

# Otherwise just run the specified command
exec "$@"
  