/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core.message;

import java.util.Set;

import org.debezium.core.doc.Array;
import org.debezium.core.doc.Document;
import org.debezium.core.doc.Value;
import org.debezium.core.util.Collect;

/**
 * @author Randall Hauch
 *
 */
public final class Message {
    
    public static final class Field {
        public static final String CLIENT_ID = "clientid";
        public static final String REQUEST = "request";
        public static final String USER = "user";
        public static final String DATABASE_ID = "dbid";
        public static final String COLLECTION = "collection";
        public static final String ZONE_ID = "zone";
        public static final String ENTITY = "entity";
        public static final String PART = "part";
        public static final String PARTS = "parts";
        public static final String BEGUN = "begun";
        public static final String PATCHES = "patches";
        public static final String INCLUDE_AFTER = "includeAfter";
        public static final String INCLUDE_BEFORE = "includeBefore";
        public static final String OPS = "ops";
        public static final String STATUS = "status";
        public static final String ERROR = "error";
        public static final String BEFORE = "before";
        public static final String AFTER = "after";
    }
    
    public static enum Status {
        SUCCESS(1), DOES_NOT_EXIST(2), PATCH_FAILED(3);
        private final int code;
        private Status( int code ) {
            this.code = code;
        }
        public int code() {
            return this.code;
        }
        public static Status fromCode( int code ) {
            switch(code) {
                case 1: return SUCCESS;
                case 2: return DOES_NOT_EXIST;
                case 3: return PATCH_FAILED;
            }
            return null;
        }
    }
    
    private static final Set<String> HEADER_FIELD_NAMES = Collect.unmodifiableSet(Field.CLIENT_ID,
                                                                                            Field.REQUEST,
                                                                                            Field.USER,
                                                                                            Field.PARTS,
                                                                                            Field.BEGUN,
                                                                                            Field.INCLUDE_BEFORE,
                                                                                            Field.INCLUDE_AFTER);
    
    /**
     * Create a new response message from the supplied request. The response will contain all of the {@link #HEADER_FIELD_NAMES header fields}
     * from the original request and a {@link Status#SUCCESS success status}.
     * @param request the original request for which the response document should be created.
     * @return the incomplete response document; never null
     */
    public static Document createResponseFrom( Document request ) {
        Document response = Document.create();
        Message.copyHeaders(request, response);
        Message.setStatus(response, Status.SUCCESS);
        return response;
    }
    
    /**
     * Create a new response message from the supplied request. The response will contain all of the {@link #HEADER_FIELD_NAMES header fields}
     * from the original request and a {@link Status#SUCCESS success status}.
     * @param batchRequest the original batch request from which the patch request document should be created.
     * @param patch the patch; may not be null
     * @return the incomplete response document; never null
     */
    public static Document createPatchRequest( Document batchRequest, Patch<?> patch ) {
        Document patchRequest = patch.asDocument();
        Message.copyHeaders(batchRequest, patchRequest);
        return patchRequest;
    }
    
    /**
     * Copy into the target document all of the header fields in the source document.
     * @param source the document with the header fields to be copied; may not be null
     * @param target the document into which copies of the header fields from the {@code source} document should be placed; may not be null
     */
    public static void copyHeaders(Document source, Document target) {
        target.putAll(source, (name)-> HEADER_FIELD_NAMES.contains(name.toString()));
    }
    
    public static void setStatus( Document message, Status status ) {
        message.setNumber(Field.STATUS,status.code());
    }
    
    public static boolean isStatus( Document message, Status status ) {
        Integer value = message.getInteger(Field.STATUS);
        return value != null && value.intValue() == status.code();
    }
    
    public static boolean isSuccess( Document message ) {
        return isStatus(message,Status.SUCCESS);
    }
    
    public static void addFailureReason( Document message, String reason ) {
        Value value = message.get(Field.ERROR);
        if ( value == null ) {
            message.setString(Field.ERROR,reason);
        } else if ( value.isArray() ) {
            value.asArray().add(reason);
        } else if ( value.isString() ) {
            message.setArray(Field.ERROR, Array.create(value.asString(),reason));
        } else {
            throw new IllegalStateException();
        }
    }
    
    public static void setAfter( Document message, Document representation ) {
        message.setDocument(Field.AFTER, representation);
    }
    
    public static void setBefore( Document message, Document representation ) {
        message.setDocument(Field.BEFORE, representation);
    }
    
    private Message() {
    }
    
}
