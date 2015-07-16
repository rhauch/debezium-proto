/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.util.concurrent.atomic.AtomicLong;

import org.debezium.core.annotation.Immutable;
import org.debezium.core.annotation.ThreadSafe;
import org.debezium.core.doc.Document;
import org.debezium.core.message.Message;

/**
 * @author Randall Hauch
 *
 */
@Immutable
@ThreadSafe
final class RequestId implements Comparable<RequestId> {
    
    private static final AtomicLong COUNTER = new AtomicLong();
    
    /**
     * Create a new request ID.
     * @param clientId the Id of the client; may not be null
     * @return the new request ID; never null
     */
    public static RequestId create(String clientId) {
        return new RequestId(clientId, COUNTER.incrementAndGet());
    }
    
    /**
     * Read the {@link RequestId} from the supplied document.
     * @param message the message that contains the request ID fields; may not be null
     * @return the request ID from the message; never null
     */
    public static RequestId from(Document message ) {
        return new RequestId(Message.getClient(message), Message.getRequest(message));
    }
    
    private final long number;
    private final String clientId;
    
    private RequestId(String clientId, long number) {
        this.clientId = clientId;
        this.number = number;
    }
    
    public String getClientId() {
        return clientId;
    }
    
    public long getRequestNumber() {
        return number;
    }
    
    public String asString() {
        return clientId + "/" + Long.toString(number);
    }
    
    @Override
    public int compareTo(RequestId that) {
        if (this == that) return 0;
        int diff = clientId.compareTo(that.clientId);
        if ( diff != 0 ) return diff;
        long offsetDiff = this.number - that.number;
        return offsetDiff == 0L ? 0 : (offsetDiff < 0L ? -1 : 1);
    }
    
    @Override
    public int hashCode() {
        return clientId.hashCode();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj instanceof RequestId) {
            RequestId that = (RequestId) obj;
            return this.clientId.equals(that.clientId) && this.number == that.number;
        }
        return false;
    }
    
    @Override
    public String toString() {
        return asString();
    }
}
