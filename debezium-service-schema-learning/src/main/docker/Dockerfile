FROM debezium-service

MAINTAINER rhauch

#
# Add this service's files to the Debezium service installation and make sure they're owned by the Debeziume user
#
ADD debezium-service-dist.tar.gz $DEBEZIUM_HOME
RUN chown -R $DEBEZIUM_USER:$DEBEZIUM_USER $DEBEZIUM_HOME

# Specify the user which should be used to execute all commands below
USER $DEBEZIUM_USER

#
# Start the service
#
CMD bin/start-service.sh
