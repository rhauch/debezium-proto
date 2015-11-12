/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.kafka;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.debezium.message.Array;
import org.debezium.message.ArraySerdes;
import org.debezium.message.Document;
import org.debezium.message.DocumentSerdes;

/**
 * @author Randall Hauch
 *
 */
public final class Serdes {

    private static final StringDeserializer STRING_DES = new StringDeserializer();
    private static final StringSerializer STRING_SER = new StringSerializer();
    private static final IntegerDeserializer INTEGER_DES = new IntegerDeserializer();
    private static final IntegerSerializer INTEGER_SER = new IntegerSerializer();
    private static final ByteArrayDeserializer BYTE_ARRAY_DES = new ByteArrayDeserializer();
    private static final ByteArraySerializer BYTE_ARRAY_SER = new ByteArraySerializer();
    private static final DocumentSerdes DOCUMENT_SERDES = new DocumentSerdes();
    private static final ArraySerdes ARRAY_SERDES = new ArraySerdes();
    
    public static Deserializer<String> stringDeserializer() {
        return STRING_DES;
    }
    
    public static Serializer<String> stringSerializer() {
        return STRING_SER;
    }
    
    public static Deserializer<Integer> integerDeserializer() {
        return INTEGER_DES;
    }
    
    public static Serializer<Integer> integerSerializer() {
        return INTEGER_SER;
    }
    
    public static Deserializer<byte[]> byteArrayDeserializer() {
        return BYTE_ARRAY_DES;
    }
    
    public static Serializer<byte[]> byteArraySerializer() {
        return BYTE_ARRAY_SER;
    }
    
    public static DocumentSerdes document() {
        return DOCUMENT_SERDES;
    }
    
    public static ArraySerdes array() {
        return ARRAY_SERDES;
    }
    
    public static String bytesToString( byte[] bytes ) {
        return STRING_DES.deserialize(null, bytes);
    }
    
    public static byte[] stringToBytes( String str ) {
        return STRING_SER.serialize(null, str);
    }
    
    public static Document bytesToDocument( byte[] bytes ) {
        return DOCUMENT_SERDES.deserialize(null, bytes);
    }
    
    public static byte[] documentToBytes( Document doc ) {
        return DOCUMENT_SERDES.serialize(null, doc);
    }

    public static Array bytesToArray( byte[] bytes ) {
        return ARRAY_SERDES.deserialize(null, bytes);
    }
    
    public static byte[] arrayToBytes( Array array ) {
        return ARRAY_SERDES.serialize(null, array);
    }
    
    private Serdes() {
    }

}
