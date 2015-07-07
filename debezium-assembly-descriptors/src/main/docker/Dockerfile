FROM debezium-jre8

MAINTAINER rhauch

ENV DEBEZIUM_HOME=/debezium \
    DEBEZIUM_USER=debezium

# Install Docker.io so that we can reuse the docker process running on the host
#RUN echo "deb http://get.docker.io/ubuntu docker main" > /etc/apt/sources.list.d/docker.list && \
#    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 36A1D7869245C8950F966E92D8576A8BA88D21E9 && \
#    apt-get update && \
#    apt-get install -y lxc-docker && \
#    rm -rf /var/lib/apt/lists/*

#
# Add the binaries, configuration files, and libraries required to run the Debezium service (via Samza without YARN),
# and remove the Hadoop/YARN lines from the 'run-class.sh' script ...
#
ADD debezium-service-baseline-dist.tar.gz $DEBEZIUM_HOME
RUN sed -i '/HADOOP/d' $DEBEZIUM_HOME/bin/run-class.sh

# Set up a user to run Debezium and own all of its files
RUN groupadd $DEBEZIUM_USER -g 1000 && \
    useradd -d $DEBEZIUM_HOME -u 1000 -g $DEBEZIUM_USER -s /bin/false -c "Debezium user" $DEBEZIUM_USER && \
    chown -R $DEBEZIUM_USER:$DEBEZIUM_USER $DEBEZIUM_HOME

# Set the working directory to the Debezium home directory
WORKDIR $DEBEZIUM_HOME

#
# Set up the volume for service data and for logs
#
VOLUME ["$DEBEZIUM_HOME/data", "$DEBEZIUM_HOME/logs"]

#
# Set up the entry point and default command
#
COPY ./docker-entrypoint.sh /
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["debezium","start"]
