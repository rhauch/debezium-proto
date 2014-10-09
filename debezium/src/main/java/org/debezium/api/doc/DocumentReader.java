/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.api.doc;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.URL;

import org.debezium.core.doc.JacksonReader;

/**
 * @author Randall Hauch
 *
 */
public interface DocumentReader {
    
    static DocumentReader defaultReader() {
        return JacksonReader.INSTANCE;
    }
    
    Document read( InputStream jsonStream ) throws IOException;
    
    Document read( Reader jsonReader ) throws IOException;
    
    Document read( String json ) throws IOException;
    
    Document read( URL jsonUrl ) throws IOException;
    
    Document read( File jsonFile ) throws IOException;
    
    default Document read( byte[] rawBytes ) throws IOException {
        try ( ByteArrayInputStream stream = new ByteArrayInputStream(rawBytes)) {
            return DocumentReader.defaultReader().read(stream);
        }
    }
    
}
