/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.message;

import java.io.IOException;

import org.debezium.Testing;
import org.debezium.message.Array;
import org.debezium.message.ArrayReader;
import org.debezium.message.ArraySerdes;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

/**
 * @author Randall Hauch
 *
 */
public class ArraySerdesTest implements Testing {
    
    private static final ArraySerdes SERDES = new ArraySerdes();
    
    @Test
    public void shouldConvertFromBytesToArray1() throws IOException {
        readAsStringAndBytes("json/array1.json");
    }

    @Test
    public void shouldConvertFromBytesToArray2() throws IOException {
        readAsStringAndBytes("json/array2.json");
    }

    protected void readAsStringAndBytes( String resourceFile ) throws IOException {
        String content = Testing.Files.readResourceAsString(resourceFile);
        Array doc = ArrayReader.defaultReader().readArray(content);
        byte[] bytes = SERDES.serialize("topicA",doc);
        Array reconstituted = SERDES.deserialize("topicA",bytes);
        assertThat((Object)reconstituted).isEqualTo(doc);
    }

}
