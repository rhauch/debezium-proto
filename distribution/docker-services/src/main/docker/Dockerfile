FROM jboss/base-jdk:8

MAINTAINER rhauch

ENV DEBEZIUM_HOME=/debezium \
    DEBEZIUM_USER=debezium

#
# Create a user and home directory for Debezium
#
USER root
RUN groupadd -r debezium -g 1001 && \
    useradd -u 1001 -r -g debezium -m -d $DEBEZIUM_HOME -s /sbin/nologin -c "Debezium user" debezium && \
    chmod 755 $DEBEZIUM_HOME

#
# Add the binaries, configuration files, and libraries required to run the Debezium services ...
#
ADD debezium-services-dist.tar.gz $DEBEZIUM_HOME
RUN chmod 755 $DEBEZIUM_HOME

USER debezium

# Set the working directory to the Debezium home directory
WORKDIR $DEBEZIUM_HOME

#
# Set up the volume for service data and for logs
#
VOLUME ["/debezium/data", "/debezium/logs"]

#
# Set up the entry point and default command
#
COPY ./docker-entrypoint.sh /
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["start"]
