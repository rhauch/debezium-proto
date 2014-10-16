/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core;

import java.util.StringJoiner;

import kafka.consumer.Blacklist;
import kafka.consumer.TopicFilter;
import kafka.consumer.Whitelist;

final class Topic {

    public static TopicFilter anyOf( String...topics) {
        StringJoiner joiner = new StringJoiner(",");
        for ( String topic : topics ) {
            joiner.add(topic);
        }
        return new Whitelist(joiner.toString());
    }

    public static TopicFilter noneOf( String...topics) {
        StringJoiner joiner = new StringJoiner(",");
        for ( String topic : topics ) {
            joiner.add(topic);
        }
        return new Blacklist(joiner.toString());
    }

    public static final DualTopic DATABASES_LIST = new DualTopic("read-dbs-requests","read-dbs-results");
    public static final DualTopic DATABASE_CHANGES = new DualTopic("database-changes","database-udpates");
    public static final DualTopic SCHEMA_CHANGES = new DualTopic("schema-changes","schema-udpates");
    
    protected static final class DualTopic {
        private final String outputTopic;
        private final String inputTopic;
        protected DualTopic( String inputTopic, String outputTopic ) {
            this.inputTopic = inputTopic;
            this.outputTopic = outputTopic;
        }
        public String inputTopic() {
            return inputTopic;
        }
        public String outputTopic() {
            return outputTopic;
        }
        public boolean isInputTopic( String topic ) {
            return inputTopic.equals(topic);
        }
        public boolean isOutputTopic( String topic ) {
            return outputTopic.equals(topic);
        }
    }
}