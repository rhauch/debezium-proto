/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core.serde;

import org.apache.samza.config.Config;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerdeFactory;
import org.debezium.core.doc.Array;
import org.debezium.core.doc.Document;

/**
 * A factory for a {@link Document} serializer and deserializer, or <em>serde</em>.
 * @author Randall Hauch
 */
public final class ArraySerdeFactory implements SerdeFactory<Array> {
    
    @Override
    public Serde<Array> getSerde(String name, Config config) {
        return Serdes.array();
    }
    
}
