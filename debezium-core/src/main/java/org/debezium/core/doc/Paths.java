/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core.doc;

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
final class Paths {
    
    static Path parse( String path, boolean resolveJsonPointerEscapes ) {
        String[] segments = path.split("/");
        if (segments.length == 0 ) return RootPath.INSTANCE;
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

    static final class RootPath implements Path {
        
        public static final Optional<Path> OPTIONAL_OF_ROOT = Optional.of(RootPath.INSTANCE);
        
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
        @Override
        public Path append(Path relPath) {
            return relPath;
        }
    }
    
    static final class SingleSegmentPath implements Path {
        private final Optional<String> segment;
        protected SingleSegmentPath( String segment ) {
            assert segment != null;
            this.segment = Optional.of(segment);    // wrap because we're always giving it away
        }
        @Override
        public Optional<Path> parent() {
            return Path.optionalRoot();
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
            return length == 1 ? this : Path.root();
        }
        @Override
        public String segment(int index) {
            if ( index >= size() || index < 0 ) throw new IllegalArgumentException("Invalid segment index: " + index);
            return segment.get();
        }
        @Override
        public Path append(Path relPath) {
            if ( relPath.isRoot() ) return this;
            String[] segments = new String[1+relPath.size()];
            segments[0] = segment.get();
            if ( relPath.isSingle() ) {
                segments[1] = relPath.segment(0);
            } else {
                MultiSegmentPath other = (MultiSegmentPath)relPath;
                System.arraycopy(other.segments, 0, segments, 1, other.segments.length);
            }
            return new MultiSegmentPath(segments);
        }
    }

    static final class MultiSegmentPath implements Path {
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
            if ( length == 0 ) return RootPath.INSTANCE;
            if ( length == 1 ) return new SingleSegmentPath(segments[0]);
            if ( length == size() ) return this;
            return new MultiSegmentPath(Arrays.copyOf(segments, length));
        }
        @Override
        public String segment(int index) {
            if ( index >= size() || index < 0 ) throw new IllegalArgumentException("Invalid segment index: " + index);
            return segments[index];
        }
        @Override
        public Path append(Path relPath) {
            if ( relPath.isRoot() ) return this;
            String[] segments = new String[size()+relPath.size()];
            System.arraycopy(this.segments, 0, segments, 1, this.segments.length);
            if ( relPath.isSingle() ) {
                segments[this.size()] = relPath.segment(0);
            } else {
                MultiSegmentPath other = (MultiSegmentPath)relPath;
                System.arraycopy(other.segments, 0, segments, 1, other.segments.length);
            }
            return new MultiSegmentPath(segments);
        }
    }
    
    private Paths() {
    }
    
}
