FROM debezium-jre8

MAINTAINER rhauch

#
# Download and install Kafka
#
ENV KAFKA_VERSION=0.8.2.1 \
    SCALA_VERSION=2.10 \
    KAFKA_HOME=/kafka \
    KAFKA_USER=kafka
RUN mkdir $KAFKA_HOME && \
    mkdir $KAFKA_HOME/data && \
    mkdir $KAFKA_HOME/logs && \
    curl -fSL -o /tmp/kafka.tgz $(curl --stderr /dev/null https://www.apache.org/dyn/closer.cgi\?as_json\=1 | sed -rn 's/.*"preferred":.*"(.*)"/\1/p')kafka/$KAFKA_VERSION/kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz

#
# Verify the contents and then install ...
#
ENV MD5HASH=446EAB1F5329EB03662926AA1CB0845D
RUN echo "$MD5HASH /tmp/kafka.tgz" | md5sum -c - &&\
    tar -xzf /tmp/kafka.tgz -C $KAFKA_HOME --strip-components 1 &&\
    rm -f /tmp/kafka.tgz

# Set up a user to run Kafka and own all of its files
RUN groupadd $KAFKA_USER -g 1000 && \
    useradd -d $KAFKA_HOME -u 1000 -g $KAFKA_USER -s /bin/false -c "Kafka user" $KAFKA_USER && \
    chown -R $KAFKA_USER:$KAFKA_USER $KAFKA_HOME

COPY ./log4j.properties $KAFKA_HOME/config/log4j.properties

# Set the working directory to the Kafka home directory
WORKDIR $KAFKA_HOME

#
# Expose the ports and set up volumes for the data and logs directories
#
EXPOSE 9092
VOLUME ["/kafka/data","/kafka/logs","/kafka/config"]

# Specify the user which should be used to execute all commands below
USER $KAFKA_USER

COPY ./docker-entrypoint.sh /
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["start"]
