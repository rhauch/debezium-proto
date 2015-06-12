#!/bin/bash

# Exit immediately if a *pipeline* returns a non-zero status. (Add -x for command tracing)
set -e

usage() {
    echo "When a container is started, it can be run with several commands. The following command"
    echo "affects the Zookeeper server:"
    echo ""
    echo "    zookeeper [start|start-foreground|stop|restart|status|print-cmd|help]"
    echo ""
    echo "The following command starts the Zookeeper command line interface, and connects it to the"
    echo "Zookeeper server that is already running within the container. This is typically used"
    echo "when attaching to an already-started container that is running the Zookeeper server."
    echo ""
    echo "    cli [param1 param2 ...]"
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
    echo "If none of these are used, then the container will start the Zookeeper server in the foreground"
    echo "and is equivalent to:"
    echo ""
    echo "    zookeeper start-foreground"
    echo ""
    echo ""
    echo "Environment variables"
    echo "---------------------"
    echo ""
    echo "You can easily control the level of logging used by Zookeeper by optionally setting these"
    echo "environment variables to 'INFO', 'WARN', 'ERROR', 'DEBUG', or 'TRACE':"
    echo ""
    echo "   ZK_LOG_LEVEL_CONSOLE      Set the level for the console log; defaults to 'INFO'."
    echo "   ZK_LOG_LEVEL_FILE         Set the level for the rolling append-only log files; defaults to 'INFO'."
    echo "                             If set to 'TRACE', the normal rolling log file will have only 'DEBUG',"
    echo "                             while all trace-level messages are recorded in a separate trace log file."
    echo ""
    echo "Other variables that start with 'ZK_' will apply to the Zookeeper configuration file."
    echo "Each environment variable name to a configuration variable name by:"
    echo ""
    echo "  1. removing the 'ZK_' prefix;"
    echo "  2. lowercasing all characters; and"
    echo "  3. converting all '_' characters to '.' characters"
    echo ""                  
    echo "For example, the environment variable 'ZK_AUTOPURGE_PURGEINTERVAL' is converted to the"
    echo "'autopurge.purgeinterval' property. If the property name exists in the service's properties"
    echo "file, the value is replaced with the environment variable's value. Otherwise, the property"
    echo "(name and value) are appended to the service's properties file."
    echo ""
    echo "The value of the environment variable may not contain a '@' character."
    echo ""
    echo ""
    echo "Volumes"
    echo "-------"
    echo ""
    echo "The container exposes two volumes:"
    echo ""
    echo "  /zookeeper/data     All Zookeeper data is written within this directory. Mount it"
    echo "                      appropriately when running your container to persist the data"
    echo "                      after the container is stopped."
    echo ""
    echo "  /zookeeper/logs     Zookeeper writes its log files within this directory."
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

# If the first argument is 'zookeeper', then shift the arguments
if [[ $1 = "zookeeper" ]]; then
    shift;
fi

if [[ -z $1 ]]; then
    ARG1="start-foreground"
    PARAMS=$ARG1
else
    ARG1=$1
    PARAMS="$@"
fi

# Process some known arguments to run Zookeeper ...
case $ARG1 in
    start|start-foreground)
        #
        # Process the logging-related environment variables. Zookeeper's log configuration allows *some* variables to be
        # set via environment variables, and more via system properties (e.g., "-Dzookeeper.console.threshold=INFO").
        # However, in the interest of keeping things straightforward and in the spirit of the immutable image, 
        # we don't use these and instead directly modify the Log4J configuration file (replacing the variables).
        #
        if [[ -z "$ZK_LOG_LEVEL_CONSOLE" ]]; then
            ZK_LOG_LEVEL_CONSOLE="INFO"
        fi
        if [[ -z "$ZK_LOG_LEVEL_FILE" ]]; then
            ZK_LOG_LEVEL_FILE="INFO"
        fi
        if [[ $ZK_LOG_LEVEL_FILE = "TRACE" ]]; then
            ZK_LOG_LEVEL_FILE="DEBUG"
            sed -i -r -e "s|\\$\\{zookeeper.root.logger\\}|TRACE, CONSOLE, ROLLINGFILE, TRACEFILE|g" $ZK_HOME/conf/log4j.properties
        else
            MIN_LOG_LEVEL=$(min_loglevel $ZK_LOG_LEVEL_FILE $ZK_LOG_LEVEL_CONSOLE)
            sed -i -r -e "s|\\$\\{zookeeper.root.logger\\}|$MIN_LOG_LEVEL, CONSOLE, ROLLINGFILE|g" $ZK_HOME/conf/log4j.properties
        fi
        sed -i -r -e "s|\\$\\{zookeeper.console.threshold\\}|$ZK_LOG_LEVEL_CONSOLE|g" $ZK_HOME/conf/log4j.properties
        sed -i -r -e "s|\\$\\{zookeeper.log.threshold\\}|$ZK_LOG_LEVEL_FILE|g" $ZK_HOME/conf/log4j.properties

        #
        # Process all environment variables that start with 'ZK_' (but not 'ZK_HOME', 'ZK_VERSION', or those that
        # start with 'ZK_LOG_'):
        #
        for VAR in `env`
        do
          env_var=`echo "$VAR" | sed -r "s/(.*)=.*/\1/g"`
          if [[ $env_var =~ ^ZK_ && $env_var =~ ^ZK_LOG_ && $env_var != "ZK_VERSION" && $env_var != "ZK_HOME" && $env_var != "ZK_USER" ]]; then
            prop_name=`echo "$VAR" | sed -r "s/ZK_(.*)=.*/\1/g" | tr '[:upper:]' '[:lower:]' | tr _ .`
            if egrep -q "(^|^#)$prop_name=" $ZK_HOME/conf/zoo.cfg; then
                #note that no config values may contain an '@' char
                sed -r -i "s@(^|^#)($prop_name)=(.*)@\2=${!env_var}@g" $ZK_HOME/conf/zoo.cfg 
            else
                echo "$prop_name=${!env_var}" >> $ZK_HOME/conf/zoo.cfg
            fi
          fi
        done
        
        # Now start the SSH daemon and the Zookeeper server
        /etc/init.d/ssh start
        exec $ZK_HOME/bin/zkServer.sh $PARAMS
        ;;
    stop)
        /etc/init.d/ssh stop
        exec $ZK_HOME/bin/zkServer.sh $PARAMS
        ;;
    restart|status|print-cmd)
        exec $ZK_HOME/bin/zkServer.sh $PARAMS
        ;;
    upgrade)
        echo "Unable to upgrade Zookeeper; use a newer Docker image if one is available"; exit 1;
        ;;
    cli)
        exec $ZK_HOME/bin/zkCli.sh -server 127.0.0.1:2181 $PARAMS
        ;;
    -h|--h|--help|help)
        usage; exit 1
        ;;
esac

# Otherwise just run the specified command
exec "$@"
