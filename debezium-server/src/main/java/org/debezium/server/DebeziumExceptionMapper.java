/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.server;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.debezium.annotation.Immutable;
import org.debezium.driver.DebeziumAuthorizationException;
import org.debezium.driver.DebeziumTimeoutException;
import org.debezium.message.Document;
import org.debezium.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Randall Hauch
 *
 */
@Immutable
@Provider
public final class DebeziumExceptionMapper implements ExceptionMapper<Throwable> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumExceptionMapper.class);

    @Override
    public Response toResponse(Throwable throwable) {
        if (throwable instanceof DebeziumAuthorizationException) {
            LOGGER.debug("Unauthorized", throwable);
            return Response.status(Status.UNAUTHORIZED)
                           .entity(documentFrom(throwable))
                           .build();
        }
        if (throwable instanceof DebeziumTimeoutException) {
            LOGGER.debug("Timed out", throwable);
            return Response.status(Status.REQUEST_TIMEOUT)
                           .entity(documentFrom(throwable))
                           .build();
        }
        return Response.status(Status.INTERNAL_SERVER_ERROR)
                       .entity(documentFrom(throwable))
                       .build();
    }

    protected Document documentFrom(Throwable t) {
        return Document.create("message", t.getMessage(), "stackTrace", Strings.getStackTrace(t));
    }
}