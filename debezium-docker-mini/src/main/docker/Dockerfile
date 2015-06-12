FROM debezium-jre8

MAINTAINER rhauch

# ************************************************************************
# NOTE:  This image is intended to be used as a simple, lightweight way to
#        run Debezium on a single machine for development purposes only.
#
#        It should NEVER be used to run Debezium in production, since only
#        a single process of each type is run, and because the data is 
#        never replicated. This is not fault tolerant nor truly durable.
#
#        All data is written to service-specific directories within the 
#        `/data` directory (e.g., `/data/zookeeper' and `/data/kakfa`.
#        All logs are written to service-specific directories within the 
#        `/log` directory (e.g., `/log/zookeeper' and `/log/kakfa`.
# ************************************************************************
#
ENV DATADIR=/data \
    LOGSDIR=/logs
RUN mkdir $DATADIR && mkdir $LOGSDIR

#
# Install Zookeeper
#
ENV ZK_VERSION=3.4.6 \
    ZK_HOME=/zookeeper \
    ZK_DATADIR=$DATADIR/zookeeper \
    ZK_LOGDIR=$LOGSDIR/zookeeper \
    ZK_LOGLEVEL=INFO \
    ZK_PORT=2181
RUN mkdir $ZK_HOME && \
    wget -q -O - $(curl --stderr /dev/null https://www.apache.org/dyn/closer.cgi\?as_json\=1 | sed -rn 's/.*"preferred":.*"(.*)"/\1/p')zookeeper/zookeeper-$ZK_VERSION/zookeeper-$ZK_VERSION.tar.gz | tar -xzf - -C $ZK_HOME --strip-components 1
RUN mkdir $ZK_DATADIR && \
    mv $ZK_HOME/conf/zoo_sample.cfg $ZK_HOME/conf/zoo.cfg && \
    sed -i -r -e "s|/tmp/zookeeper|$ZK_DATADIR|g" \
              -e 's|#autopurge|autopurge|g' \
              $ZK_HOME/conf/zoo.cfg && \
    mkdir $ZK_LOGDIR && \
    sed -i -r -e "s|(zookeeper.root.logger)=*|\1=$ZK_LOGLEVEL CONSOLE, ROLLINGFILE|g" \
              -e "s|(zookeeper.log.dir)=*|\1=$ZK_LOGDIR/|g" \
              -e "s|(zookeeper.tracelog.dir)=\.|\1=$ZK_LOGDIR/|g" \
              -e 's|#(log4j.appender.ROLLINGFILE.MaxBackupIndex.*)|\1|g' \
              $ZK_HOME/conf/log4j.properties

#
# Install Kafka
#
ENV KAFKA_VERSION=0.8.2.1 \
    SCALA_VERSION=2.10 \
    KAFKA_HOME=/kafka \
    KAFKA_DATADIR=$DATADIR/kafka \
    KAFKA_LOGDIR=$LOGSDIR/kafka \
    KAKFA_PORT=9092

RUN mkdir $KAFKA_HOME && \
    wget -q -O - $(curl --stderr /dev/null https://www.apache.org/dyn/closer.cgi\?as_json\=1 | sed -rn 's/.*"preferred":.*"(.*)"/\1/p')kafka/$KAFKA_VERSION/kafka_$SCALA_VERSION-$KAFKA_VERSION.tgz | tar -xzf - -C $KAFKA_HOME --strip-components 1

#
# Expose the ports and set up volumes for the data and logs directories
#
EXPOSE $ZK_PORT
VOLUME $DATADIR $LOGSDIR

# Start the multiple processes ...
CMD $ZK_HOME/bin/zkServer.sh start-foreground
