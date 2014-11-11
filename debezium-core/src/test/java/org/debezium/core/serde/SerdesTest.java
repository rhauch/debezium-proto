/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core.serde;

import static org.fest.assertions.Assertions.assertThat;

import java.io.IOException;

import org.debezium.Testing;
import org.debezium.core.doc.Document;
import org.debezium.core.doc.DocumentReader;
import org.junit.Test;

/**
 * @author Randall Hauch
 *
 */
public class SerdesTest implements Testing {

    @Test
    public void shouldUseSerdeMethodToConvertFromBytesToDocument1() throws IOException {
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
        byte[] binary = Serdes.stringToBytes(content);
        Document expected = DocumentReader.defaultReader().read(content);
        Document doc = Serdes.bytesToDocument(binary);
        assertThat((Object)doc).isEqualTo(expected);
    }

}
