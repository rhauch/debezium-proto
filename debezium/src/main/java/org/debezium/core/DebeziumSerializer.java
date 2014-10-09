/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import kafka.serializer.Decoder;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

import org.debezium.api.doc.Document;
import org.debezium.api.doc.DocumentReader;
import org.debezium.api.doc.DocumentWriter;

/**
 * 
 * This is a very simple serializer mechanism that simply encodes and decodes JSON documents as UTF-8 strings.
 * @author Randall Hauch
 */
public class DebeziumSerializer implements Encoder<Object>, Decoder<Object> {

    public DebeziumSerializer( VerifiableProperties props ) {
        // Required
    }
    
    @Override
    public byte[] toBytes(Object message) {
        Document doc = (Document)message;
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            DocumentWriter.defaultWriter().write(doc, stream);
            return stream.toByteArray();
        } catch ( IOException e ) {
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public Object fromBytes(byte[] bytes) {
        try ( ByteArrayInputStream stream = new ByteArrayInputStream(bytes)) {
            return DocumentReader.defaultReader().read(stream);
        } catch ( IOException e ) {
            throw new RuntimeException(e);
        }
    }
}
