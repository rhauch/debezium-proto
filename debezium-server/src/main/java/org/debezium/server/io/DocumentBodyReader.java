/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.server.io;

import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;

import org.debezium.annotation.Immutable;
import org.debezium.message.Document;
import org.debezium.message.DocumentReader;

/**
 * A {@link MessageBodyReader} implementation that supports reading {@link Document}s.
 * 
 * @author Randall Hauch
 */
@Immutable
@Produces({ MediaType.APPLICATION_JSON })
public final class DocumentBodyReader implements MessageBodyReader<Document> {

    private final DocumentReader reader;

    public DocumentBodyReader(DocumentReader reader) {
        assert reader != null;
        this.reader = reader;
    }
    
    @Override
    public boolean isReadable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return mediaType == MediaType.APPLICATION_JSON_TYPE && Document.class.isAssignableFrom(type);
    }
    
    @Override
    public Document readFrom(Class<Document> type, Type genericType, Annotation[] annotations, MediaType mediaType,
                             MultivaluedMap<String, String> httpHeaders, InputStream entityStream) throws IOException,
            WebApplicationException {
        return reader.read(entityStream);
    }
}
