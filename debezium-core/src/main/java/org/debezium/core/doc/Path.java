/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core.doc;

import java.util.Optional;

/**
 * @author Randall Hauch
 *
 */
public interface Path extends Iterable<String> {
    
    public static interface Segments {
        public static boolean isAfterLastIndex( String segment ) {
            return "-".equals(segment);
        }

        public static boolean isArrayIndex( String segment ) {
            return isAfterLastIndex(segment) || asInteger(segment).isPresent();
        }

        public static boolean isFieldName( String segment ) {
            return !isArrayIndex(segment);
        }
        public static Optional<Integer> asInteger( String segment ) {
            try {
                return Optional.of(Integer.parseInt(segment));
            } catch ( NumberFormatException e ) {
                return Optional.empty();
            }
        }
        public static Optional<Integer> asInteger( Optional<String> segment ) {
            return segment.isPresent() ? asInteger(segment.get()) : Optional.empty();
        }
    }
    
    public static Path root() {
        return Paths.RootPath.INSTANCE;
    }
    
    static Optional<Path> optionalRoot() {
        return Paths.RootPath.OPTIONAL_OF_ROOT;
    }
    
    static Path parse( String path ) {
        return Paths.parse(path, true);
    }
    
    static Path parse( String path, boolean resolveJsonPointerEscapes ) {
        return Paths.parse(path, resolveJsonPointerEscapes);
    }
    
    default boolean isRoot() {
        return size() == 0;
    }

    default boolean isSingle() {
        return size() == 1;
    }

    int size();
    
    Optional<Path> parent();
    
    Optional<String> lastSegment();
    
    Path subpath( int length );
    
    String segment(int index );
    
    default Path append( String relPath ) {
        return append(Path.parse(relPath));
    }
    
    String toRelativePath();
    
    Path append( Path relPath );
}
