/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.api.doc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;

import org.debezium.core.doc.JacksonWriter;

/**
 * @author Randall Hauch
 *
 */
public interface DocumentWriter {
    
    static DocumentWriter defaultWriter() {
        return JacksonWriter.INSTANCE;
    }
    
    default byte[] writeAsBytes( Document document ) {
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            write(document, stream);
            return stream.toByteArray();
        } catch ( IOException e ) {
            // This really should never happen ...
            e.printStackTrace();
            return new byte[]{};
        }
    }
    
    void write( Document document, OutputStream jsonStream ) throws IOException;
    
    void write( Document document, Writer jsonWriter ) throws IOException;
    
    String write( Document document ) throws IOException;

}
