/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core.message;

import java.util.Set;

import org.debezium.core.doc.Document;
import org.debezium.core.doc.Value;
import org.debezium.core.util.CollectionFactory;

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
    
    private static final Set<String> HEADER_FIELD_NAMES = CollectionFactory.unmodifiableSet(Field.CLIENT_ID,
                                                                                            Field.REQUEST,
                                                                                            Field.USER,
                                                                                            Field.PARTS,
                                                                                            Field.BEGUN,
                                                                                            Field.INCLUDE_BEFORE,
                                                                                            Field.INCLUDE_AFTER);
    
    public static void copyHeaders(Document source, Document target) {
        HEADER_FIELD_NAMES.forEach((name)->{
            Value value = source.get(name);
            if ( value != null ) target.set(name,value);
        });
    }
    
    private Message() {
    }
    
}
