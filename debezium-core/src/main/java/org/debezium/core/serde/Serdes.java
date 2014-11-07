/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core.serde;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.samza.serializers.Serde;
import org.debezium.core.annotation.Immutable;
import org.debezium.core.doc.Document;
import org.debezium.core.doc.DocumentReader;
import org.debezium.core.doc.DocumentWriter;

/**
 * @author Randall Hauch
 *
 */
public final class Serdes {
    
    private static final DocumentSerde DOCUMENT_SERDE_INSTANCE = new DocumentSerde();
    private static final StringSerde STRING_SERDE_INSTANCE = new StringSerde();
    private static final DocumentReader DOCUMENT_READER = DocumentReader.defaultReader();
    private static final DocumentWriter DOCUMENT_WRITER = DocumentWriter.defaultWriter();
    
    public static Serde<Document> document() {
        return DOCUMENT_SERDE_INSTANCE;
    }

    public static Encoder<Document> documentEncoder() {
        return DOCUMENT_SERDE_INSTANCE;
    }

    public static Decoder<Document> documentDecoder() {
        return DOCUMENT_SERDE_INSTANCE;
    }

    public static Serde<String> string() {
        return STRING_SERDE_INSTANCE;
    }

    public static Encoder<String> stringEncoder() {
        return STRING_SERDE_INSTANCE;
    }

    public static Decoder<String> stringDecoder() {
        return STRING_SERDE_INSTANCE;
    }

    public static String bytesToString( byte[] bytes ) {
        return new String(bytes,StandardCharsets.UTF_8);
    }
    
    public static byte[] stringToBytes( String str ) {
        return str.getBytes(StandardCharsets.UTF_8);
    }
    
    public static Document bytesToDocument( byte[] bytes ) {
        try {
            return DOCUMENT_READER.read(bytesToString(bytes));
        } catch (IOException e) {
            // Should never see this, but shit if we do ...
            throw new RuntimeException(e);
        }
    }
    
    public static byte[] documentToBytes( Document doc ) {
        try {
            return stringToBytes(DOCUMENT_WRITER.write(doc));
        } catch (IOException e) {
            // Should never see this, but shit if we do ...
            throw new RuntimeException(e);
        }
    }

    @Immutable
    private static final class DocumentSerde implements Serde<Document>, Encoder<Document>, Decoder<Document> {
        @Override
        public Document fromBytes(byte[] bytes) {
            return bytesToDocument(bytes);
        }
        
        @Override
        public byte[] toBytes(Document document) {
            return documentToBytes(document);
        }
    }

    @Immutable
    private static final class StringSerde implements Serde<String>, Encoder<String>, Decoder<String> {
        @Override
        public String fromBytes(byte[] bytes) {
            return bytesToString(bytes);
        }
        
        @Override
        public byte[] toBytes(String document) {
            return stringToBytes(document);
        }
    }

    private Serdes() {
    }

}
