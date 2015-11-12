/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.message;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongPredicate;
import java.util.stream.Collectors;

import org.debezium.annotation.Immutable;
import org.debezium.model.DatabaseId;
import org.debezium.model.EntityId;
import org.debezium.model.EntityType;
import org.debezium.model.Identifier;
import org.debezium.model.ZoneId;
import org.debezium.model.ZoneSubscription.Interest;
import org.debezium.util.Collect;

/**
 * A utility for working with Debezium message documents.
 * 
 * @author Randall Hauch
 */
@Immutable
public final class Message {

    private static final AtomicLong REQUEST_NUMBER = new AtomicLong();

    public static final class Field {
        public static final String CLIENT_ID = "clientid";
        public static final String REQUEST = "request";
        public static final String USER = "user";
        public static final String DEVICE = "device";
        public static final String APPLICATION_VERSION = "appVersion";
        public static final String DATABASE_ID = "db";
        public static final String COLLECTION = "collection";
        public static final String ZONE_ID = "zone";
        public static final String ENTITY = "entity";
        public static final String PART = "part";
        public static final String PARTS = "parts";
        public static final String BEGUN = "begun";
        public static final String ENDED = "ended";
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
        public static final String ACTION = "action";
        public static final String FIELDS = "fields";
        public static final String ENTITY_TYPE = "$type";
        public static final String ENTITY_TAGS = "$tags";
        public static final String ENTITY_ETAG = "$etag";
        public static final String ENTITY_VCLOCK = "$vclock";
        public static final String ENTITY_DOC = "$rep";
        public static final String REVISION = "$rev";
        public static final String METRICS = "$metrics";
    }

    public static enum Action {
        CREATED("created", Interest.ON_CREATE.code()),
        UPDATED("updated", Interest.ON_UPDATE.code()),
        DELETED("deleted", Interest.ON_DELETE.code());
        private static final Map<String, Action> BY_DESCRIPTION = new HashMap<>();
        static {
            for (Action action : Action.values()) {
                BY_DESCRIPTION.put(action.description(), action);
            }
        }
        private final String desc;
        private final int interestCode;

        private Action(String desc, int interestCode) {
            this.desc = desc;
            this.interestCode = interestCode;
        }

        public String description() {
            return desc;
        }

        public static Action find(String description) {
            return description != null ? BY_DESCRIPTION.get(description.toLowerCase()) : null;
        }

        public boolean satisfies(Interest interest) {
            return satisfies(interest.code());
        }

        public boolean satisfies(int interestMask) {
            return (interestCode & interestMask) == interestCode;
        }
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
                                                                                  Field.PART,
                                                                                  Field.PARTS,
                                                                                  Field.DATABASE_ID,
                                                                                  Field.COLLECTION,
                                                                                  Field.ZONE_ID,
                                                                                  Field.ENTITY,
                                                                                  Field.BEGUN,
                                                                                  Field.LEARNING,
                                                                                  Field.INCLUDE_BEFORE,
                                                                                  Field.INCLUDE_AFTER);

    private static final Set<String> REQUIRED_HEADER_FIELD_NAMES = Collect.unmodifiableSet(Field.CLIENT_ID,
                                                                                           Field.REQUEST,
                                                                                           Field.USER,
                                                                                           Field.DATABASE_ID,
                                                                                           Field.BEGUN);

    public static Set<String> headerFields() {
        return HEADER_FIELD_NAMES;
    }

    public static Set<String> requiredHeaderFields() {
        return REQUIRED_HEADER_FIELD_NAMES;
    }

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
        Message.setEnded(response, System.currentTimeMillis());
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
        complete.setNumber(Field.BEGUN, System.currentTimeMillis());
        complete.setNull(Field.ENDED);
        int parts = complete.getInteger(Field.PARTS);
        Array responses = complete.setArray(Field.RESPONSES, Array.createWithNulls(parts));
        Value part = partialResponse.get(Field.PART);
        assert part != null;
        assert part.isNumber();
        int index = part.asInteger().intValue() - 1;
        responses.setDocument(index, partialResponse);
        return complete;
    }

    /**
     * Add the supplied partial response to the given aggregate response message.
     * 
     * @param aggregateResponse the aggregate response document; may not be null
     * @param partialResponse the partial response to be added to the aggregate response; may not be null
     * @param completionTimestamp the timestamp that should be used when this aggregate response is completed
     * @return true if the aggregate response is complete and has all of the partial responses, or false otherwise
     */
    public static boolean addToAggregateResponse(Document aggregateResponse, Document partialResponse, long completionTimestamp ) {
        // Set only some of the headers ...
        assert aggregateResponse.getString(Field.CLIENT_ID).equals(partialResponse.getString(Field.CLIENT_ID));
        assert aggregateResponse.getLong(Field.REQUEST).equals(partialResponse.getLong(Field.REQUEST));
        assert aggregateResponse.getString(Field.USER).equals(partialResponse.getString(Field.USER));
        assert aggregateResponse.getInteger(Field.PARTS).equals(partialResponse.getInteger(Field.PARTS));
        Array responses = aggregateResponse.getArray(Field.RESPONSES);
        assert responses != null;
        Value part = partialResponse.get(Field.PART);
        assert part != null;
        assert part.isInteger();
        int index = part.asInteger().intValue() - 1;
        responses.setDocument(index, partialResponse);
        if (responses.streamValues().allMatch(Value::isNotNull)) {
            // The aggregate is complete ...
            aggregateResponse.setNumber(Field.ENDED, completionTimestamp);
            return true;
        }
        return false;
    }
    
    /**
     * Determine whether the supplied aggregate response has been completed and is expired according to the given expiry predicate.
     * @param aggregateResponse the aggregate response document; may not be null
     * @param expiryFunction the function that is to be used to determine if the response is expired given the completion timestamp
     * @return {@code true} if the aggregate response is complete and expired, or {@code false} otherwise
     */
    public static boolean isAggregateResponseCompletedAndExpired( Document aggregateResponse, LongPredicate expiryFunction ) {
        // Determine if this is complete ...
        Long ended = aggregateResponse.getLong(Field.ENDED);
        return ended == null ? false : expiryFunction.test(ended.longValue());
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
        Array responses = response.getArray(Field.RESPONSES);
        if (responses != null) {
            // It is a partial response ...
            responses.streamValues().forEach(value -> handlePartialResponse(function, value.asDocument()));
        } else {
            // Must not be an aggregate response ...
            handlePartialResponse(function, response);
        }
    }

    private static void handlePartialResponse(PartialResponseHandler function, Document partialResponse) {
        Identifier id = getId(partialResponse);
        long request = getRequest(partialResponse);
        assert id != null;
        int part = getPart(partialResponse);
        function.accept(id, part, request, partialResponse);
    }

    /**
     * A functional interface used to process each partial response in an aggregate response document.
     * 
     * @see Message#forEachPartialResponse(Document, PartialResponseHandler)
     * @author Randall Hauch
     */
    @FunctionalInterface
    public static interface PartialResponseHandler {
        /**
         * Accept the partial response document with the given identifier, request number, and partial response document.
         * 
         * @param id the target identifier of the partial response; never null
         * @param partialNumber the 1-based number signifying which partial in the aggregate is provided
         * @param request the client-specific request number of the aggregate request
         * @param response the partial response document; never null
         */
        void accept(Identifier id, int partialNumber, long request, Document response);
    }

    /**
     * Create a new response message from the supplied request. The response will contain all of the {@link #HEADER_FIELD_NAMES
     * header fields} from the original request and a {@link Status#SUCCESS success status}.
     * 
     * @param batchRequest the original batch request from which the patch request document should be created.
     * @param patch the patch; may not be null
     * @return the incomplete response document; never null
     */
    public static Document createPatchRequest(Document batchRequest, Patch<? extends Identifier> patch) {
        Document patchRequest = patch.asDocument();
        copyHeaders(batchRequest, patchRequest);
        addId(batchRequest, patch.target());
        return patchRequest;
    }

    public static Identifier getId(Document doc, DatabaseId databaseId) {
        if (databaseId == null) return getId(doc);
        if (doc == null) return null;
        String entityType = doc.getString(Field.COLLECTION);
        if (entityType == null) return databaseId;
        String zoneId = doc.getString(Field.ZONE_ID);
        if (zoneId == null) return Identifier.of(databaseId, entityType);
        String id = doc.getString(Field.ENTITY);
        if (id == null) return Identifier.zone(databaseId, entityType, zoneId);
        return Identifier.of(databaseId, entityType, id, zoneId);
    }

    public static Identifier getId(Document doc) {
        if (doc == null) return null;
        String databaseId = doc.getString(Field.DATABASE_ID);
        if (databaseId == null) return null;
        String entityType = doc.getString(Field.COLLECTION);
        if (entityType == null) return Identifier.of(databaseId);
        String zoneId = doc.getString(Field.ZONE_ID);
        if (zoneId == null) return Identifier.of(databaseId, entityType);
        String id = doc.getString(Field.ENTITY);
        if (id == null) return Identifier.zone(databaseId, entityType, zoneId);
        return Identifier.of(databaseId, entityType, id, zoneId);
    }

    public static EntityId getEntityId(Document doc) {
        String databaseId = doc.getString(Field.DATABASE_ID);
        String entityType = doc.getString(Field.COLLECTION);
        String zoneId = doc.getString(Field.ZONE_ID);
        String id = doc.getString(Field.ENTITY);
        return Identifier.of(databaseId, entityType, id, zoneId);
    }

    public static EntityType getEntityType(Document doc) {
        String databaseId = doc.getString(Field.DATABASE_ID);
        String entityType = doc.getString(Field.COLLECTION);
        return Identifier.of(databaseId, entityType);
    }

    /**
     * Get the database identifier from the supplied document.
     * 
     * @param doc the document; may not be null
     * @return the database ID, or null if the identifier could not be found
     */
    public static DatabaseId getDatabaseId(Document doc) {
        String databaseId = doc.getString(Field.DATABASE_ID);
        return databaseId != null ? Identifier.of(databaseId) : null;
    }

    public static ZoneId getZoneId(Document doc, DatabaseId dbId) {
        if (dbId == null) {
            String databaseId = doc.getString(Field.DATABASE_ID);
            dbId = Identifier.of(databaseId);
        }
        String entityType = doc.getString(Field.COLLECTION);
        String zoneId = doc.getString(Field.ZONE_ID);
        return Identifier.zone(dbId, entityType, zoneId);
    }

    public static void addId(Document doc, Identifier id) {
        id.fields().forEachRemaining((field) -> doc.set(field.getName(), field.getValue()));
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
        target.putAll(source, HEADER_FIELD_NAMES::contains);
    }

    /**
     * Copy into the target document the {@link Field#ENDED ended} time in the source message, which is the time
     * at which processing was completed on the source. This method does nothing if the source does not have
     * an {@link Field#ENDED ended} time field.
     * 
     * @param source the document with the {@link Field#ENDED ended} time to be copied; may not be null
     * @param target the document on which the {@link Field#ENDED ended} time should be set; may not be null
     */
    public static void copyCompletionTime(Document source, Document target) {
        Long timestamp = source.getLong(Field.ENDED);
        if (timestamp != null) target.setNumber(Field.ENDED, timestamp.longValue());
    }

    public static Document createConnectionMessage(String clientId, DatabaseId dbId, String user, String device, String appVersion) {
        return createConnectionMessage(clientId, dbId, user, device, appVersion, System.currentTimeMillis());
    }

    public static Document createConnectionMessage(String clientId, DatabaseId dbId, String user, String device, String appVersion,
                                                   long timestamp) {
        Document doc = Document.create();
        doc.setString(Field.CLIENT_ID, clientId);
        doc.setString(Field.DATABASE_ID, dbId.asString());
        doc.setString(Field.USER, user);
        doc.setString(Field.DEVICE, device);
        doc.setString(Field.APPLICATION_VERSION, appVersion);
        doc.setNumber(Field.BEGUN, timestamp);
        return doc;
    }

    public static String getUser(Document message) {
        return message.getString(Field.USER);
    }

    public static String getDevice(Document message) {
        return message.getString(Field.DEVICE);
    }

    public static void removeDevice(Document message) {
        message.remove(Field.DEVICE);
    }

    public static String getAppVersion(Document message) {
        return message.getString(Field.APPLICATION_VERSION);
    }

    public static OptionalLong getBegun(Document message) {
        Value value = message.get(Field.BEGUN);
        return value.isLong() ? OptionalLong.of(value.asLong()) : OptionalLong.empty();
    }

    public static void setBegun(Document message, long timestamp) {
        message.setNumber(Field.BEGUN, timestamp);
    }

    public static void setEnded(Document message, long timestamp) {
        message.setNumber(Field.ENDED, timestamp);
    }
    
    public static long getEnded(Document message) {
        return message.getLong(Field.ENDED, 0L);
    }

    public static void setRevision(Document message, long revision) {
        message.setNumber(Field.REVISION, revision);
    }

    public static long getRevision(Document message) {
        return message.getLong(Field.REVISION, 0L);
    }

    /**
     * Get the {@link Status} in the supplied message.
     * 
     * @param message the response message
     * @return the status, or null if there is no (known) status
     */
    public static Status getStatus(Document message) {
        return Status.fromCode(message.getInteger(Field.STATUS, -1));
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
        if (Value.isNull(value)) {
            message.setString(Field.ERROR, reason);
        } else if (value.isArray()) {
            value.asArray().add(reason);
        } else if (value.isString()) {
            message.setArray(Field.ERROR, Array.create(value.asString(), reason));
        } else {
            throw new IllegalStateException("Unexpected value: " + value);
        }
    }

    public static Collection<String> getFailureReasons(Document message) {
        Value value = message.get(Field.ERROR);
        if (Value.notNull(value)) {
            if (value.isArray()) {
                return value.asArray().streamValues().filter(Value::notNull)
                            .map(Value::toString)
                            .collect(Collectors.toList());
            }
            if (value.isDocument()) {
                return value.asDocument().stream().filter(Document.Field::isNotNull)
                            .map(Document.Field::getValue)
                            .map(Value::toString)
                            .collect(Collectors.toList());
            }
            return Collections.singleton(value.toString());
        }
        return Collections.emptyList();
    }

    public static Optional<String> getFirstFailureReason(Document message) {
        Value value = message.get(Field.ERROR);
        if (Value.notNull(value)) {
            if (value.isArray()) {
                return value.asArray().streamValues().filter(Value::notNull)
                            .map(Value::toString)
                            .findFirst();
            }
            if (value.isDocument()) {
                return value.asDocument().stream().filter(Document.Field::isNotNull)
                            .map(Document.Field::getValue)
                            .map(Value::toString)
                            .findFirst();
            }
            return Optional.of(value.toString());
        }
        return Optional.empty();
    }

    public static void setOperations(Document response, Document originalRequest) {
        Value value = originalRequest.get(Field.OPS);
        if (Value.notNull(value)) response.set(Field.OPS, value);
    }

    public static void setOperations(Document message, Patch<?> patch) {
        setOperations(message, patch.asDocument());
    }

    public static String getClient(Document message) {
        return message.getString(Field.CLIENT_ID);
    }

    public static void setClient(Document message, String clientId) {
        message.setString(Field.CLIENT_ID, clientId);
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
        return message.getDocument(Field.AFTER);
    }

    public static Document getBefore(Document message) {
        return message.getDocument(Field.BEFORE);
    }

    public static Document getAfterOrBefore(Document message) {
        Document result = getAfter(message);
        return result != null ? result : getBefore(message);
    }

    public static boolean includeAfter(Document message) {
        return message.getBoolean(Field.INCLUDE_AFTER, false);
    }

    public static boolean includeBefore(Document message) {
        return message.getBoolean(Field.INCLUDE_BEFORE, false);
    }

    public static long getDurationInMillis(Document message) {
        long begun = message.getLong(Field.BEGUN, -1);
        long ended = message.getLong(Field.ENDED, -1);
        if (begun <= 0L || ended <= 0L) return -1;
        assert ended >= begun;
        return ended - begun;
    }

    public static int getParts(Document message) {
        return message.getInteger(Field.PARTS, 1);
    }

    public static int getParts(Document message, int defaultParts) {
        return message.getInteger(Field.PARTS, defaultParts);
    }

    public static int getPart(Document message) {
        return message.getInteger(Field.PART, 1);
    }

    public static int getPart(Document message, int defaultPart) {
        return message.getInteger(Field.PART, defaultPart);
    }

    public static void setParts(Document message, int part, int parts) {
        assert part <= part;
        assert part >= 0;
        assert parts >= 0;
        message.setNumber(Field.PART, part);
        message.setNumber(Field.PARTS, parts);
    }

    public static boolean hasOps(Document message) {
        Array array = message.getArray(Field.OPS);
        return array != null && !array.isEmpty();
    }

    public static boolean isReadOnly(Document message) {
        Array array = message.getArray(Field.OPS);
        return array == null || array.isEmpty();
    }

    public static Action determineAction(Document message) {
        boolean includesBefore = getBefore(message) != null;
        if (includesBefore) {
            boolean includesAfter = getAfter(message) != null;
            return includesAfter ? Action.UPDATED : Action.DELETED;
        }
        return Action.CREATED;
    }

    private Message() {
    }

}
