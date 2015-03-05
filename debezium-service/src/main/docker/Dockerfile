FROM debezium-jre8

MAINTAINER rhauch

ENV DEBEZIUM_HOME /opt/debezium
ENV DEBEZIUM_USER debezium

#
# Add the binaries, configuration files, and libraries required to run the Debezium service (via Samza without YARN)
#
ADD debezium-service-baseline-dist.tar.gz $DEBEZIUM_HOME

# Set up a user to run Debezium
RUN groupadd $DEBEZIUM_USER && \
  useradd -d $DEBEZIUM_HOME -g $DEBEZIUM_USER -s /bin/false $DEBEZIUM_USER && \
  chown -R $DEBEZIUM_USER:$DEBEZIUM_USER $DEBEZIUM_HOME/config $DEBEZIUM_HOME/logs 
USER $DEBEZIUM_USER

WORKDIR $DEBEZIUM_HOME

#
# Set up the ENV for the Kakfa and ZK host and ports
#

#
# Set up the volume for state data and for logs
#
VOLUME ["$DEBEZIUM_HOME/data", "$DEBEZIUM_HOME/logs"]
