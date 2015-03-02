FROM jeanblanchard/busybox-java:8

MAINTAINER rhauch

ENV DEBEZIUM_HOME /debezium

#
# Add the binaries, configuration files, and libraries required to run a Samza service (without YARN)
#
ADD debezium-samza-service-baseline-contents.tar.gz $DEBEZIUM_HOME