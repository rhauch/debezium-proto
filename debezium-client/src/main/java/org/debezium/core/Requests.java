/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core;

import org.debezium.core.doc.Document;


/**
 * @author Randall Hauch
 *
 */
public class Requests {
    
    public static Document readSchemaRequest( ExecutionContext context, RequestId requestId, long requestTime ) {
        Document request = Document.create();
        request.setString("requestId",requestId.asString());
        request.setString("requestor",context.username());
        request.setNumber("requestedAt",requestTime);
        request.setString("dbId", context.databaseId().asString());
        return request;
    }
    
//    public static Outcome<Schema> readSchemaResponse( Document message ) {
//
//    }
    

    private Requests() {
    }
    
}
