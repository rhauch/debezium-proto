/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.server.io;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;

import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;

import org.debezium.annotation.Immutable;
import org.debezium.message.ArrayWriter;
import org.debezium.message.Patch;

/**
 * A {@link MessageBodyWriter} implementation that supports writing {@link Patch} objects.
 * 
 * @author Randall Hauch
 */
@Immutable
@Produces({ MediaType.APPLICATION_JSON })
public final class PatchBodyWriter implements MessageBodyWriter<Patch<?>> {

    private final ArrayWriter writer;

    public PatchBodyWriter(ArrayWriter writer) {
        assert writer != null;
        this.writer = writer;
    }

    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return mediaType == MediaType.APPLICATION_JSON_TYPE && Patch.class.isAssignableFrom(type);
    }

    @Override
    public void writeTo(Patch<?> patch, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType,
                        MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream) throws IOException, WebApplicationException {
        PrintStream ps = new PrintStream(new BufferedOutputStream(entityStream), true, "UTF-8");
        ps.print(writer.write(patch.asArray()));
        httpHeaders.putSingle("Content-Type", mediaType.toString() + ";charset=utf-8");
    }

    @Override
    public long getSize(Patch<?> patch, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        try {
            return writer.write(patch.asArray()).getBytes(StandardCharsets.UTF_8).length;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
