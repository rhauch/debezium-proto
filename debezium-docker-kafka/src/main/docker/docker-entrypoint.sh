#!/bin/bash

usage() {
    echo "When a container is started, it can be run with several commands. The following command"
    echo "will start a Kafka broker:"
    echo ""
    echo "  start"
    echo ""
    echo "while the following command can be used when attaching to a running container to"
    echo ""
    echo "  create-topic [-p numPartitions] [-r numReplicas] topic"
    echo ""
    echo "where 'topic' is the name of the new topic, 'numPartitions' is the number of partitions within"
    echo "the new topic, and 'numReplicas' is the number of replicas for each partition within the"
    echo "new topic. By default the topic will be created with one partition and one replica."
    echo ""
    echo "This help message is displayed with the following command:"
    echo ""
    echo "  help"
    echo ""
    echo "Finally, the container can run arbitrary commands. For example, to start a new container"
    echo "or attach to a running container and obtain a bash shell:"
    echo ""
    echo "  bash"
    echo ""
    echo "If none of these are used, then the container will start the Kafka broker."
    echo ""
    echo ""
    echo "Environment variables"
    echo "---------------------"
    echo ""
    echo "You can pass multiple environment variables to alter the Kafka configuration:"
    echo ""
    echo "   KAFKA_BROKER_ID           Recommended. Set this to the unique and persistent number for the broker."
    echo "                             This must be set for every broker in a Kafka cluster, and should be"
    echo "                             set for a single standalone broker. The default is '1'."
    echo "   KAFKA_ZOOKEEPER_CONNECT   Recommended. Set this to the 'zookeeper.connect' string described"
    echo "                             in the Kafka documentation so that the Kafka broker can find the"
    echo "                             Zookeeper broker. If this container is started with a link to another"
    echo "                             container running Zookeeper on port 2181, then this environment"
    echo "                             variable need not be set since it can be determined automatically."
    echo "                             Otherwise, it should be set with an explicit value."
    echo "   KAFKA_ADVERTISED_PORT     Recommended. Set this to the port number on the docker host running"
    echo "                             this container that Kafka should advertise to consumers, producers"
    echo "                             and other brokers, if it is different than 9092 that Kafka uses"
    echo "                             by default."
    echo "   KAFKA_HEAP_OPTS           Recommended. Use this to set the JVM options for the Kafka broker."
    echo "                             By default a value of '-Xmx1G -Xms1G' is used, meaning that each"
    echo "                             Kafka broker uses 1GB of memory. Using too little memory may cause"
    echo "                             performance problems, while using too much may prevent the broker"
    echo "                             from starting properly given the memory available on the machine."
    echo "   KAFKA_CREATE_TOPICS       Optional. Use this to specify the topics that should be created"
    echo "                             as soon as the broker starts. The value should be a comma-separated"
    echo "                             list of topics, partitions, and replicas. For example, the value"
    echo "                             'topic1:1:2,topic2:3:1' will create 'topic1' with 1 partition and"
    echo "                             2 replicas, and 'topic2' with 3 partitions and 1 replica."
    echo ""
    echo "Other variables that start with 'KAFKA_' will also apply to the Kafka configuration file."
    echo "Each environment variable name to a configuration variable name by:"
    echo ""
    echo "  1. removing the 'KAFKA_' prefix;"
    echo "  2. lowercasing all characters; and"
    echo "  3. converting all '_' characters to '.' characters"
    echo ""                  
    echo "For example, the environment variable 'KAFKA_ADVERTISED_HOST_NAME' is converted to the"
    echo "'advertised.host.name' property, while 'KAFKA_AUTO_CREATE_TOPICS_ENABLE' is converted to"
    echo "the 'auto.create.topics.enable' property. If the property name exists in the service's properties"
    echo "file, the value is replaced with the environment variable's value. Otherwise, the property"
    echo "(name and value) are appended to the broker's properties file."
    echo ""
    echo "The value of the environment variable may not contain a '\@' character."
    echo ""
    echo ""
    echo "Volumes"
    echo "-------"
    echo ""
    echo "The container exposes two volumes:"
    echo ""
    echo "  /kafka/data     The broker writes all persisted data as files within this directory."
    echo "                  Mount it appropriately when running your container to persist the data"
    echo "                  after the container is stopped."
    echo ""
    echo "  /kafka/logs     The broker places its log files within this directory, in a subdirectory"
    echo "                  named with the broker ID (see KAFKA_BROKER_ID above)."
    echo ""
}

# Exit immediately if a *pipeline* returns a non-zero status. (Add -x for command tracing)
set -e

if [[ -z "$KAFKA_BROKER_ID" ]]; then
    echo "WARNING: The KAFKA_BROKER_ID environment variable should be set. Defaulting to '1'"
    echo ""
    export KAFKA_BROKER_ID="1"
fi
if [[ -z "$KAFKA_ADVERTISED_PORT" ]]; then
    echo "WARNING: Advertising default port 9092 to consumers, producers, and other brokers. If other port is desired, set KAFKA_ADVERTISED_PORT environment variable."
    echo ""
    export KAFKA_ADVERTISED_PORT=9092
fi
if [[ -z "$KAFKA_LOG_DIRS" ]]; then
    export KAFKA_LOG_DIRS="$KAFKA_HOME/logs/$KAFKA_BROKER_ID"
    mkdir -p $KAFKA_HOME/logs/$KAFKA_BROKER_ID    
fi
if [[ -z "$KAFKA_ZOOKEEPER_CONNECT" ]]; then
    # Look for any environment variables set by Docker container linking. For example, if the container
    # running Zookeeper were named 'zoo' in this container, then Docker should have created several envs,
    # such as 'ZOO_PORT_2181_TCP'. If so, then use that to automatically set the 'zookeeper.connect' property.
    export KAFKA_ZOOKEEPER_CONNECT=$(env | grep .*PORT_2181_TCP= | sed -e 's|.*tcp://||' | paste -sd ,)
fi
if [[ -n "$KAFKA_HEAP_OPTS" ]]; then
    sed -r -i "s/^(export KAFKA_HEAP_OPTS)=\"(.*)\"/\1=\"${KAFKA_HEAP_OPTS}\"/g" $KAFKA_HOME/bin/kafka-server-start.sh
    unset KAFKA_HEAP_OPTS
fi

#
# Process all environment variables that start with 'KAFKA_' (but not 'KAFKA_HOME' OR 'KAFKA_VERSION'):
#
for VAR in `env`
do
  env_var=`echo "$VAR" | sed -r "s/(.*)=.*/\1/g"`
  if [[ $env_var =~ ^KAFKA_ && $env_var != "KAFKA_VERSION" && $env_var != "KAFKA_HOME" && $env_var != "KAFKA_CREATE_TOPICS" && $env_var != "KAFKA_USER" ]]; then
    prop_name=`echo "$VAR" | sed -r "s/^KAFKA_(.*)=.*/\1/g" | tr '[:upper:]' '[:lower:]' | tr _ .`
    if egrep -q "(^|^#)$prop_name=" $KAFKA_HOME/config/server.properties; then
        #note that no config names or values may contain an '@' char
        sed -r -i "s@(^|^#)($prop_name)=(.*)@\2=${!env_var}@g" $KAFKA_HOME/config/server.properties
    else
        #echo "Adding property $prop_name=${!env_var}"
        echo "$prop_name=${!env_var}" >> $KAFKA_HOME/config/server.properties
    fi
  fi
done

# If the first argument is 'kafka', then shift the arguments
if [[ $1 = "kafka" ]]; then
    shift;
fi

# Process the argument to this container ...
case $1 in
    start)
        if [[ -n $KAFKA_CREATE_TOPICS ]]; then
            # Start a subshell in the background that waits for the Kafka broker to open socket on port 9092 and 
            # then creates the topics when the broker is running and able to receive connections ...
            (
                echo "START: Waiting for Kafka broker to open socket on port 9092 ..."
                while ss | awk '$5 ~ /:9092$/ {exit 1}'; do sleep 1; done
                echo "START: Found running Kafka broker on port 9092, so creating topics ..."
                IFS=','; for topicToCreate in $KAFKA_CREATE_TOPICS; do
                    IFS=':' read -a topicConfig <<< "$topicToCreate"
                    echo "START: Creating topic ${topicConfig[0]} with ${topicConfig[1]} partitions and ${topicConfig[2]} replicas ..."
                    $KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper $KAFKA_ZOOKEEPER_CONNECT --replication-factor ${topicConfig[2]} --partition ${topicConfig[1]} --topic "${topicConfig[0]}"
                done
            )&
        fi
        exec $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties
        ;;
    create-topic)
        shift
        PARTITION=1
        REPLICAS=1
        while getopts :p:r: option; do
            case ${option} in
                p)
                    PARTITION=$OPTARG
                    ;;
                r)
                    REPLICAS=$OPTARG
                    ;;
                h|\?)
                    usage; exit 1;
                    ;;
            esac
        done
        shift $((OPTIND -1))
        TOPICNAME=$1
        exec $KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper $KAFKA_ZOOKEEPER_CONNECT --replication-factor $REPLICAS --partition $PARTITION --topic "$TOPICNAME"
        ;;
    help)
        usage; exit 1;
        ;;
esac

# Otherwise just run the specified command
exec "$@"
