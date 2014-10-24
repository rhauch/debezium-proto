/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core.serde;

import java.io.IOException;

import org.apache.samza.config.Config;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerdeFactory;
import org.debezium.core.annotation.Immutable;
import org.debezium.core.doc.Document;
import org.debezium.core.doc.DocumentReader;
import org.debezium.core.doc.DocumentWriter;

/**
 * A factory for a {@link Document} serializer and deserializer, or <em>serde</em>.
 * @author Randall Hauch
 */
public final class DocumentSerdeFactory implements SerdeFactory<Document> {
    
    @Override
    public Serde<Document> getSerde(String name, Config config) {
        return SHARED_INSTANCE;
    }
    
    private static final DocumentSerde SHARED_INSTANCE = new DocumentSerde();
    private static final DocumentReader SHARED_READER = DocumentReader.defaultReader();
    private static final DocumentWriter SHARED_WRITER = DocumentWriter.defaultWriter();
    
    @Immutable
    private static final class DocumentSerde implements Serde<Document> {
        @Override
        public Document fromBytes(byte[] bytes) {
            try {
                return SHARED_READER.read(bytes);
            } catch ( IOException e ) {
                throw new RuntimeException(e);
            }
        }
        
        @Override
        public byte[] toBytes(Document document) {
            return SHARED_WRITER.writeAsBytes(document);
        }
    }
    
}
