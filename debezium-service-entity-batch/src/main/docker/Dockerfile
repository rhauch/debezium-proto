FROM debezium-service

MAINTAINER rhauch

#
# Add this service's files to the Debezium service installation
#
USER root
ADD debezium-service-dist.tar.gz $DEBEZIUM_HOME
USER debezium

#
# Start the service
#
CMD ["$DEBEZIUM_HOME/bin/start-service.sh"]
