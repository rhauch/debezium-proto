/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Randall Hauch
 *
 */
public abstract class RequestId implements Comparable<RequestId> {
    
    private static final AtomicLong COUNTER = new AtomicLong();
    
    static RequestId from(String id) {
        return new SimpleRequestId(id);
    }
    
    static RequestId create() {
        return new SimpleRequestId(UUID.randomUUID().toString());
    }
    
    static RequestId create( String clientId ) {
        return new SimpleRequestId(clientId,COUNTER.incrementAndGet());
    }
    
    public abstract String asString();
    
    private static final class SimpleRequestId extends RequestId {
        private final String id;
        private SimpleRequestId(String clientId, long offset ) {
            this.id = clientId + "/" + Long.toString(offset);
        }
        private SimpleRequestId(String id ) {
            this.id = id;
        }
        @Override
        public String asString() {
            return id;
        }
        @Override
        public int compareTo(RequestId that) {
            if ( this == that ) return 0;
            if ( that instanceof SimpleRequestId ) {
                return id.compareTo(((SimpleRequestId)that).id);
            }
            return asString().compareTo(that.asString());
        }
        @Override
        public int hashCode() {
            return id.hashCode();
        }
        @Override
        public boolean equals(Object obj) {
            if ( obj == this ) return true;
            if ( obj instanceof SimpleRequestId ) {
                SimpleRequestId that = (SimpleRequestId)obj;
                return this.id.equals(that.id);
            }
            if ( obj instanceof RequestId ) {
                RequestId that = (RequestId)obj;
                return this.asString().equals(that.asString());
            }
            return false;
        }
    }

}
