#!/bin/bash

# Exit immediately if a *pipeline* returns a non-zero status. (Add -x for command tracing)
set -e

usage() {
    echo "When a container is started, it can be run with several commands. To following command"
    echo "starts the Debezium service:"
    echo ""
    echo "    debezium [start|help]"
    echo ""
    echo "This message is displayed with the following command:"
    echo ""
    echo "    help"
    echo ""
    echo "Finally, the container can run arbitrary commands. For example, to start a new container"
    echo "or attach to a running container and obtain a bash shell:"
    echo ""
    echo "    bash"
    echo ""
    echo "If none of these are used, then the container will start the Debezium service in the foreground"
    echo "and is equivalent to:"
    echo ""
    echo "    debezium start"
    echo ""
    echo ""
    echo "Environment variables"
    echo "---------------------"
    echo ""
    echo "You can easily control the level of logging used by Debezium by optionally setting these"
    echo "environment variables to 'INFO', 'WARN', 'ERROR', 'DEBUG', or 'TRACE':"
    echo ""
    echo "   LOG_LEVEL_CONSOLE      Set the level for the console log; defaults to 'INFO'."
    echo "   LOG_LEVEL_FILE         Set the level for the rolling append-only log files; defaults to 'INFO'."
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

loglevel_to_value() {
    case $1 in
       ERROR)   echo "1";;
       WARN)    echo "2";;
       INFO|*)  echo "3";;
       DEBUG)   echo "4";;
       TRACE)   echo "5";;
    esac
}

min_loglevel() {
    LEVEL1=$(loglevel_to_value $1)
    LEVEL2=$(loglevel_to_value $2)
    if [[ "$LEVEL1" -eq "$LEVEL2" ]] || [[ "$LEVEL1" -gt "$LEVEL2" ]]; then
       echo $1
    else
       echo $2
    fi
}

if [[ -n "$ZOOKEEPER" ]]; then
  export DEBEZIUM_SYSTEMS_KAFKA_CONSUMER_ZOOKEEPER_CONNECT=$ZOOKEEPER
fi

if [[ -n "$KAFKA" ]]; then
  export DEBEZIUM_SYSTEMS_KAFKA_PRODUCER_BOOTSTRAP_SERVERS=$KAFKA
fi

if [[ -n "$CHECKPOINT_INTERVAL_SECONDS" ]]; then
  export DEBEZIUM_TASK_COMMIT_MS=$(echo $(($CHECKPOINT_INTERVAL_SECONDS * 1000)) )
fi

if [[ -n "$CHECKPOINT_REPLICATION" ]]; then
  export DEBEZIUM_TASK_CHECKPOINT_REPLICATION_FACTOR=$CHECKPOINT_REPLICATION
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

# If the first argument is 'debezium' or "*-service", then shift the arguments
if [[ $1 = "debezium" ]] || [[ $1 == *"-service" ]]; then
    shift;
fi

# Process the argument to this container ...
case $1 in
    start)
        #
        # Process the logging-related environment variables. Zookeeper's log configuration allows *some* variables to be
        # set via environment variables, and more via system properties (e.g., "-Dzookeeper.console.threshold=INFO").
        # However, in the interest of keeping things straightforward and in the spirit of the immutable image, 
        # we don't use these and instead directly modify the Log4J configuration file (replacing the variables).
        #
        if [[ -z "$LOG_LEVEL_CONSOLE" ]]; then
            LOG_LEVEL_CONSOLE="INFO"
        fi
        if [[ -z "$LOG_LEVEL_FILE" ]]; then
            LOG_LEVEL_FILE="INFO"
        fi
        MIN_LOG_LEVEL=$(min_loglevel $LOG_LEVEL_FILE $LOG_LEVEL_CONSOLE)
        ROOT_LOGGER="$MIN_LOG_LEVEL, console, rolling"
        sed -i -r -e "s|\\$\\{debezium.root.logger\\}|$ROOT_LOGGER|g" \
                     "s|\\$\\{debezium.console.threshold\\}|$LOG_LEVEL_CONSOLE|g" \
                     "s|\\$\\{debezium.rolling.threshold\\}|$LOG_LEVEL_FILE|g" \
                     $DEBEZIUM_HOME/config/log4j.properties
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
        # Start the service ...        
        #
        export JAVA_OPTS="-Xloggc:$LOG_DIR/gc.log "`
                        `"-Dlog4j.configuration=file:$LOG_CONFIG "`
                        `"-Ddebezium.logs.dir=$LOG_DIR "`
                        `"-Ddebezium.root.logger=\"$ROOT_LOGGER\" "`
                        `"-Ddebezium.console.threshold=LOG_LEVEL_CONSOLE "`
                        `"-Ddebezium.rolling.threshold=LOG_LEVEL_FILE "`
                        `"-Dsamza.container.name=$CONTAINER_NAME"
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
  