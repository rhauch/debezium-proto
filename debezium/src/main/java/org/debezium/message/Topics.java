/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.message;

import java.util.StringJoiner;

import kafka.consumer.Blacklist;
import kafka.consumer.TopicFilter;
import kafka.consumer.Whitelist;

import org.debezium.annotation.Immutable;

/**
 * The collection of Debezium message topics.
 * 
 * @author Randall Hauch
 */
@Immutable
public interface Topics {

    public static TopicFilter of( String topic ) {
        return new Whitelist(topic);
    }

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
}
