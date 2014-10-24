/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core.message;

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.debezium.core.component.Identifier;
import org.debezium.core.doc.Array;
import org.debezium.core.doc.Document;
import org.debezium.core.doc.Value;
import org.debezium.core.util.Collect;

/**
 * @author Randall Hauch
 *
 */
public final class Message {
    
    private static final AtomicLong REQUEST_NUMBER = new AtomicLong();
    
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
        public static final String LEARNING = "learning";
        public static final String PATCHES = "patches";
        public static final String INCLUDE_AFTER = "includeAfter";
        public static final String INCLUDE_BEFORE = "includeBefore";
        public static final String OPS = "ops";
        public static final String STATUS = "status";
        public static final String ERROR = "error";
        public static final String BEFORE = "before";
        public static final String AFTER = "after";
        public static final String RESPONSES = "responses";
    }
    
    public static enum Status {
        SUCCESS(1), DOES_NOT_EXIST(2), PATCH_FAILED(3);
        private final int code;
        
        private Status(int code) {
            this.code = code;
        }
        
        public int code() {
            return this.code;
        }
        
        public static Status fromCode(int code) {
            switch (code) {
                case 1:
                    return SUCCESS;
                case 2:
                    return DOES_NOT_EXIST;
                case 3:
                    return PATCH_FAILED;
            }
            return null;
        }
    }
    
    private static final Set<String> HEADER_FIELD_NAMES = Collect.unmodifiableSet(Field.CLIENT_ID,
                                                                                  Field.REQUEST,
                                                                                  Field.USER,
                                                                                  Field.PARTS,
                                                                                  Field.BEGUN,
                                                                                  Field.LEARNING,
                                                                                  Field.INCLUDE_BEFORE,
                                                                                  Field.INCLUDE_AFTER);
    
    /**
     * Create a new response message from the supplied request. The response will contain all of the {@link #HEADER_FIELD_NAMES
     * header fields} from the original request and a {@link Status#SUCCESS success status}.
     * 
     * @param request the original request for which the response document should be created.
     * @return the incomplete response document; never null
     */
    public static Document createResponseFromRequest(Document request) {
        Document response = Document.create();
        Message.copyHeaders(request, response);
        Message.setStatus(response, Status.SUCCESS);
        return response;
    }
    
    /**
     * Create a new aggregate response message from the supplied partial response.
     * 
     * @param partialResponse the partial response for which the complete response document should be created; may not be null
     * @return the aggregate response document; never null
     */
    public static Document createAggregateResponseFrom(Document partialResponse) {
        Document complete = Document.create();
        // Set only some of the headers ...
        complete.setString(Field.CLIENT_ID, partialResponse.getString(Field.CLIENT_ID));
        complete.setNumber(Field.REQUEST, partialResponse.getLong(Field.REQUEST));
        complete.setString(Field.USER, partialResponse.getString(Field.USER));
        complete.setNumber(Field.PARTS, partialResponse.getInteger(Field.PARTS));
        Document responses = complete.getOrCreateDocument(Field.RESPONSES);
        Value part = partialResponse.get(Field.PART);
        assert part != null;
        assert part.isInteger();
        responses.setDocument(part.convert().asString(), partialResponse);
        return complete;
    }
    
    /**
     * Add to the aggregate response message from the supplied partial response.
     * 
     * @param aggregateResponse the aggregate response document; may not be null
     * @param partialResponse the partial response to be added to the aggregate response; may not be null
     * @return true if the aggregate response is complete and has all of the partial responses, or false otherwise
     */
    public static boolean addToAggregateResponse(Document aggregateResponse, Document partialResponse) {
        Document complete = Document.create();
        // Set only some of the headers ...
        assert aggregateResponse.getString(Field.CLIENT_ID).equals(partialResponse.getString(Field.CLIENT_ID));
        assert aggregateResponse.getLong(Field.REQUEST) == partialResponse.getLong(Field.REQUEST);
        assert aggregateResponse.getString(Field.USER).equals(partialResponse.getString(Field.USER));
        assert aggregateResponse.getInteger(Field.PARTS) == partialResponse.getInteger(Field.PARTS);
        int parts = aggregateResponse.getInteger(Field.PARTS);
        Document responses = complete.getDocument(Field.RESPONSES);
        assert responses != null;
        Value part = partialResponse.get(Field.PART);
        assert part != null;
        assert part.isInteger();
        assert !responses.has(part.convert().asString());
        responses.setDocument(part.convert().asString(), partialResponse);
        return parts >= responses.size();
    }
    
    /**
     * Given the supplied aggregate or partial response message, invoke the function for each of the partial responses.
     * If the given document is an aggregate, the function will be called once for each contained partial response. If the
     * given document is a partial response, the function will be called once with the supplied partial document.
     * 
     * @param response the aggregate or partial response document; may not be null
     * @param function the consumer function that should be called for each partial response document; may not be null
     */
    public static void forEachPartialResponse(Document response, PartialResponseHandler function) {
        Document responses = response.getDocument(Field.RESPONSES);
        if (responses != null) {
            // It is a partial response ...
            responses.forEach((field) -> handlePartialResponse(function,field.getValue().asDocument()));
        } else {
            // Must not be an aggregate response ...
            handlePartialResponse(function,response);
        }
    }
    
    private static void handlePartialResponse(PartialResponseHandler function, Document partialResponse ) {
        Identifier id = Identifier.parse(partialResponse);
        long request = getRequest(partialResponse);
        assert id != null;
        function.accept(id,request,partialResponse);
    }

    /**
     * A functional interface used to process each partial response in an aggregate response document.
     * @see Message#forEachPartialResponse(Document, PartialResponseHandler)
     * @author Randall Hauch
     */
    @FunctionalInterface
    public static interface PartialResponseHandler {
        /**
         * Accept the partial response document with the given identifier, request number, and partial response document.
         * @param id the target identifier of the partial response; never null
         * @param request the client-specific request number of the aggregate request
         * @param response the partial response document; never null
         */
        void accept( Identifier id, long request, Document response );
    }
    
    /**
     * Create a new response message from the supplied request. The response will contain all of the {@link #HEADER_FIELD_NAMES
     * header fields} from the original request and a {@link Status#SUCCESS success status}.
     * 
     * @param batchRequest the original batch request from which the patch request document should be created.
     * @param patch the patch; may not be null
     * @return the incomplete response document; never null
     */
    public static Document createPatchRequest(Document batchRequest, Patch<?> patch) {
        Document patchRequest = patch.asDocument();
        Message.copyHeaders(batchRequest, patchRequest);
        return patchRequest;
    }
    
    public static void addHeaders(Document doc, String clientId) {
        addHeaders(doc, clientId, REQUEST_NUMBER.incrementAndGet(), null, System.currentTimeMillis());
    }
    
    public static void addHeaders(Document doc, String clientId, long request, String user) {
        addHeaders(doc, clientId, request, user, System.currentTimeMillis());
    }
    
    public static void addHeaders(Document doc, String clientId, long request, String user, long timestamp) {
        assert clientId != null;
        doc.setString(Field.CLIENT_ID, clientId);
        doc.setNumber(Field.REQUEST, request);
        if (user != null) doc.setString(Field.USER, user);
        if (timestamp > 0L) doc.setNumber(Field.BEGUN, timestamp);
    }
    
    /**
     * Copy into the target document all of the header fields in the source document.
     * 
     * @param source the document with the header fields to be copied; may not be null
     * @param target the document into which copies of the header fields from the {@code source} document should be placed; may
     *            not be null
     */
    public static void copyHeaders(Document source, Document target) {
        target.putAll(source, (name) -> HEADER_FIELD_NAMES.contains(name.toString()));
    }
    
    public static void setStatus(Document message, Status status) {
        message.setNumber(Field.STATUS, status.code());
    }
    
    public static boolean isStatus(Document message, Status status) {
        Integer value = message.getInteger(Field.STATUS);
        return value != null && value.intValue() == status.code();
    }
    
    public static boolean isSuccess(Document message) {
        return isStatus(message, Status.SUCCESS);
    }
    
    public static void addFailureReason(Document message, String reason) {
        Value value = message.get(Field.ERROR);
        if (value == null) {
            message.setString(Field.ERROR, reason);
        } else if (value.isArray()) {
            value.asArray().add(reason);
        } else if (value.isString()) {
            message.setArray(Field.ERROR, Array.create(value.asString(), reason));
        } else {
            throw new IllegalStateException();
        }
    }
    
    public static String getClient(Document message) {
        return message.getString(Field.CLIENT_ID);
    }
    
    public static boolean isFromClient(Document message, String clientId) {
        Value value = message.get(Field.CLIENT_ID);
        return value != null && value.isString() && value.asString().equals(clientId);
    }
    
    public static long getRequest(Document message) {
        Long value = message.getLong(Field.REQUEST);
        assert value != null;
        return value.longValue();
    }
    
    public static void setLearning(Document message, boolean enabled) {
        if (enabled) {
            message.setBoolean(Field.LEARNING, true);
        } else {
            message.remove(Field.LEARNING);
        }
    }
    
    public static boolean isLearningEnabled(Document message) {
        return message.getBoolean(Field.LEARNING, false);
    }
    
    public static void setAfter(Document message, Document representation) {
        message.setDocument(Field.AFTER, representation);
    }
    
    public static void setBefore(Document message, Document representation) {
        message.setDocument(Field.BEFORE, representation);
    }
    
    public static Document getAfter(Document message) {
        return message.setDocument(Field.AFTER);
    }
    
    public static Document getBefore(Document message) {
        return message.setDocument(Field.BEFORE);
    }
    
    public static boolean includeAfter(Document message) {
        return message.getBoolean(Field.INCLUDE_AFTER, false);
    }
    
    public static boolean includeBefore(Document message) {
        return message.getBoolean(Field.INCLUDE_BEFORE, false);
    }
    
    public static int getParts(Document message) {
        return message.getInteger(Field.PARTS, 1);
    }
    
    public static void setParts(Document message, int part, int parts) {
        assert part <= part;
        assert part >= 0;
        assert parts >= 0;
        message.setNumber(Field.PART, part);
        message.setNumber(Field.PARTS, parts);
    }
    
    private Message() {
    }
    
}
