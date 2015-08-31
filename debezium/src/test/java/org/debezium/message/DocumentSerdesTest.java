/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.message;

import java.io.IOException;

import org.debezium.Testing;
import org.debezium.message.Document;
import org.debezium.message.DocumentReader;
import org.debezium.message.DocumentSerdes;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

/**
 * @author Randall Hauch
 *
 */
public class DocumentSerdesTest implements Testing {
    
    private static final DocumentSerdes SERDES = new DocumentSerdes();
    
    @Test
    public void shouldConvertFromBytesToDocument1() throws IOException {
        readAsStringAndBytes("json/sample1.json");
    }

    @Test
    public void shouldUseSerdeMethodToConvertFromBytesToDocument2() throws IOException {
        readAsStringAndBytes("json/sample2.json");
    }
    
    @Test
    public void shouldUseSerdeMethodToConvertFromBytesToDocument3() throws IOException {
        readAsStringAndBytes("json/sample3.json");
    }
    
    @Test
    public void shouldUseSerdeMethodToConvertFromBytesToDocumentForResponse1() throws IOException {
        readAsStringAndBytes("json/response1.json");
    }
    
    @Test
    public void shouldUseSerdeMethodToConvertFromBytesToDocumentForResponse2() throws IOException {
        readAsStringAndBytes("json/response2.json");
    }
    
    protected void readAsStringAndBytes( String resourceFile ) throws IOException {
        String content = Testing.Files.readResourceAsString(resourceFile);
        Document doc = DocumentReader.defaultReader().read(content);
        byte[] bytes = SERDES.serialize("topicA",doc);
        Document reconstituted = SERDES.deserialize("topicA",bytes);
        assertThat((Object)reconstituted).isEqualTo(doc);
    }

}
