/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core.doc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.io.Writer;

import org.debezium.api.doc.Array;
import org.debezium.api.doc.Document;
import org.debezium.api.doc.DocumentWriter;
import org.debezium.api.doc.Value;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

/**
 * @author Randall Hauch
 *
 */
public final class JacksonWriter implements DocumentWriter {
    
    public static final JacksonWriter INSTANCE = new JacksonWriter();
    
    private static final JsonFactory factory;
    
    static {
        factory = new JsonFactory();
        // factory.enable(JsonGenerator.Feature.ALLOW_COMMENTS);
    }
    
    @Override
    public void write(Document document, OutputStream jsonStream) throws IOException {
        writeDocument(document, factory.createGenerator(jsonStream));
    }
    
    @Override
    public void write(Document document, Writer jsonWriter) throws IOException {
        writeDocument(document, factory.createGenerator(jsonWriter));
    }
    
    @Override
    public String write(Document document) throws IOException {
        StringWriter writer = new StringWriter();
        writeDocument(document, factory.createGenerator(writer));
        return writer.getBuffer().toString();
    }
    
    @Override
    public byte[] writeAsBytes(Document document) {
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            writeDocument(document, factory.createGenerator(stream, JsonEncoding.UTF8));
            return stream.toByteArray();
        } catch (IOException e ) {
            throw new RuntimeException(e);
        }
    }
    
    protected static void writeDocument(Document document, JsonGenerator generator) throws IOException {
        generator.writeStartObject();
        try {
            document.stream().forEach((field) -> {
                try {
                    generator.writeFieldName(field.toString());
                    writeValue(field.getValue(), generator);
                } catch (IOException e) {
                    throw new WritingError(e);
                }
            });
            generator.writeEndObject();
        } catch (WritingError e) {
            throw e.wrapped();
        } finally {
            generator.close();
        }
    }
    
    protected static void writeArray(Array array, JsonGenerator generator) throws IOException {
        generator.writeStartArray();
        try {
            array.streamValues().forEach((value) -> {
                try {
                    writeValue(value, generator);
                } catch (IOException e) {
                    throw new WritingError(e);
                }
            });
            generator.writeEndArray();
        } catch (WritingError e) {
            throw e.wrapped();
        }
    }
    
    protected static void writeValue(Value value, JsonGenerator generator) throws IOException {
        switch (value.getType()) {
            case NULL:
                generator.writeNull();
                break;
            case STRING:
                generator.writeString(value.asString());
                break;
            case BOOLEAN:
                generator.writeBoolean(value.asBoolean());
                break;
            case BINARY:
                generator.writeBinary(value.asBytes());
                break;
            case INTEGER:
                generator.writeNumber(value.asInteger());
                break;
            case LONG:
                generator.writeNumber(value.asInteger());
                break;
            case FLOAT:
                generator.writeNumber(value.asInteger());
                break;
            case DOUBLE:
                generator.writeNumber(value.asInteger());
                break;
            case BIG_INTEGER:
                generator.writeNumber(value.asBigInteger());
                break;
            case DECIMAL:
                generator.writeNumber(value.asBigDecimal());
                break;
            case DOCUMENT:
                writeDocument(value.asDocument(), generator);
            case ARRAY:
                writeArray(value.asArray(), generator);
        }
    }
    
    protected static final class WritingError extends RuntimeException {
        private static final long serialVersionUID = 1L;
        private final IOException wrapped;
        
        protected WritingError(IOException wrapped) {
            this.wrapped = wrapped;
        }
        
        public IOException wrapped() {
            return wrapped;
        }
    }
    
}
