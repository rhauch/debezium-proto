/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.api.doc;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.Consumer;

import org.debezium.core.util.Iterators;
import org.debezium.core.util.Joiner;

/**
 * @author Randall Hauch
 *
 */
public abstract class Path implements Iterable<String> {
    
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
        return RootPath.INSTANCE;
    }
    
    private static final Optional<Path> OPTIONAL_OF_ROOT = Optional.of(RootPath.INSTANCE);
    
    protected static Optional<Path> optionalRoot() {
        return OPTIONAL_OF_ROOT;
    }
    
    public static Path parse( String path ) {
        return parse(path,true);
    }
    
    public static Path parse( String path, boolean resolveJsonPointerEscapes ) {
        String[] segments = path.split("/");
        if (segments.length == 0 ) return root();
        if (segments.length == 1 ) return new SingleSegmentPath(parseSegment(segments[0],resolveJsonPointerEscapes));
        if ( resolveJsonPointerEscapes ) {
            for ( int i=0; i!=segments.length; ++i ) segments[i] = parseSegment(segments[i],true);
        }
        return new MultiSegmentPath(segments);
    }
    
    private static String parseSegment( String segment,boolean resolveJsonPointerEscapes ) {
        if ( resolveJsonPointerEscapes ) {
            segment = segment.replaceAll("\\~1", "/").replaceAll("\\~0", "~");
        }
        return segment;
    }
    
    public boolean isRoot() {
        return size() == 0;
    }

    public boolean isSingle() {
        return size() == 1;
    }

    public abstract int size();
    
    public abstract Optional<Path> parent();
    
    public abstract Optional<String> lastSegment();
    
    public abstract Path subpath( int length );
    
    public abstract String segment(int index );

    @Override
    public abstract void forEach(Consumer<? super String> consumer);

    
    protected static final class RootPath extends Path {
        public static final Path INSTANCE = new RootPath();
        private RootPath() {
        }
        @Override
        public Optional<Path> parent() {
            return Optional.empty();
        }
        @Override
        public Optional<String> lastSegment() {
            return Optional.empty();
        }
        @Override
        public int size() {
            return 0;
        }
        @Override
        public String toString() {
            return "/";
        }
        @Override
        public Iterator<String> iterator() {
            return Iterators.empty();
        }
        @Override
        public void forEach(Consumer<? super String> consumer) {
        }
        @Override
        public Path subpath(int length) {
            if ( length != 0 ) throw new IllegalArgumentException("Invalid subpath length: " + length);
            return this;
        }
        @Override
        public String segment(int index ) {
            throw new IllegalArgumentException("Invalid segment index: " + index);
        }
    }
    
    protected static final class SingleSegmentPath extends Path {
        private final Optional<String> segment;
        protected SingleSegmentPath( String segment ) {
            this.segment = Optional.of(segment);
        }
        @Override
        public Optional<Path> parent() {
            return optionalRoot();
        }
        @Override
        public Optional<String> lastSegment() {
            return segment;
        }
        @Override
        public int size() {
            return 1;
        }
        @Override
        public String toString() {
            return "/" + segment.get();
        }
        @Override
        public Iterator<String> iterator() {
            return Iterators.with(segment.get());
        }
        @Override
        public void forEach(Consumer<? super String> consumer) {
            consumer.accept(segment.get());
        }
        @Override
        public Path subpath(int length) {
            if ( length > size() || length < 0 ) throw new IllegalArgumentException("Invalid subpath length: " + length);
            return length == 1 ? this : root();
        }
        @Override
        public String segment(int index) {
            if ( index >= size() || index < 0 ) throw new IllegalArgumentException("Invalid segment index: " + index);
            return segment.get();
        }
    }

    protected static final class MultiSegmentPath extends Path {
        private final String[] segments;
        protected MultiSegmentPath( String[] segments ) {
            this.segments = segments;
            assert size() > 1;
        }
        @Override
        public Optional<Path> parent() {
            if ( size() == 2 ) return Optional.of(new SingleSegmentPath(segments[0]));
            return Optional.of(new MultiSegmentPath(Arrays.copyOf(segments, segments.length-1)));
        }
        @Override
        public Optional<String> lastSegment() {
            return Optional.of(segments[segments.length-1]);
        }
        @Override
        public int size() {
            return segments.length;
        }
        @Override
        public String toString() {
            return Joiner.on("/","/").join(segments);
        }
        @Override
        public Iterator<String> iterator() {
            return Iterators.with(segments);
        }
        @Override
        public void forEach(Consumer<? super String> consumer) {
            for ( String segment : segments ) {
                consumer.accept(segment);
            }
        }
        @Override
        public Path subpath(int length) {
            if ( length > size() || length < 0 ) throw new IllegalArgumentException("Invalid subpath length: " + length);
            if ( length == 0 ) return root();
            if ( length == 1 ) return new SingleSegmentPath(segments[0]);
            if ( length == size() ) return this;
            return new MultiSegmentPath(Arrays.copyOf(segments, length));
        }
        @Override
        public String segment(int index) {
            if ( index >= size() || index < 0 ) throw new IllegalArgumentException("Invalid segment index: " + index);
            return segments[index];
        }
    }
}
