/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core.doc;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.URL;

/**
 * @author Randall Hauch
 *
 */
public interface ArrayReader {
    
    static ArrayReader defaultReader() {
        return JacksonReader.INSTANCE;
    }
    
    Array readArray( InputStream jsonStream ) throws IOException;
    
    Array readArray( Reader jsonReader ) throws IOException;
    
    Array readArray( String json ) throws IOException;
    
    Array readArray( URL jsonUrl ) throws IOException;
    
    Array readArray( File jsonFile ) throws IOException;
    
    default Array readArray( byte[] rawBytes ) throws IOException {
        try ( ByteArrayInputStream stream = new ByteArrayInputStream(rawBytes)) {
            return ArrayReader.defaultReader().readArray(stream);
        }
    }
    
}
