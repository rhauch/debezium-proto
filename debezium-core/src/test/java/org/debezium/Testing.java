/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium;

import static org.fest.assertions.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.EnumSet;
import java.util.Set;

import org.debezium.core.util.IoUtil;
import org.fest.assertions.Fail;
import org.junit.Before;

/**
 * A set of utility methods for test cases.
 * @author Randall Hauch
 */
public interface Testing {
    
    @Before
    default void resetBeforeEachTest() {
        Print.enabled = false;
        Debug.enabled = false;
    }
    
    public static final class Print {
        private static boolean enabled = false;
        
        public static void enable() {
            enabled = true;
        }

        public static void disable() {
            enabled = true;
        }
        
        public static boolean isEnabled() {
            return enabled;
        }
    }
    
    public static void print( Object message ) {
        if ( message != null && Print.enabled ) {
            System.out.println(message);
        }
    }
    
    public static final class Debug {
        private static boolean enabled = false;
        
        public static void enable() {
            enabled = true;
        }

        public static void disable() {
            enabled = true;
        }
        
        public static boolean isEnabled() {
            return enabled;
        }
    }
    
    public static void debug( Object message ) {
        if ( message != null && Debug.enabled ) {
            System.out.println(message);
        }
    }
    
    public static void printError( Object message ) {
        if ( message != null ) {
            System.err.println(message);
        }
    }
    
    public static void printError( Throwable throwable ) {
        if ( throwable != null ) {
            throwable.printStackTrace();
        }
    }
    
    public static interface Files {
        
        public static InputStream readResourceAsStream( String pathOnClasspath ) {
            InputStream stream = Testing.class.getClassLoader().getResourceAsStream(pathOnClasspath);
            assertThat(stream).isNotNull();
            return stream;
        }
        
        public static String readResourceAsString( String pathOnClasspath ) {
            try ( InputStream stream = readResourceAsStream(pathOnClasspath)) {
                return IoUtil.read(stream);
            } catch ( IOException e ) {
                Fail.fail("Unable to read '" + pathOnClasspath + "'", e);
                return null;
            }
        }
        
        /**
         * A method that will delete a file or folder only if it is within the 'target' directory (for safety).
         * Folders are removed recursively.
         * @param path the path to the file or folder in the target directory
         * @throws IOException if there is a problem deleting the files at this path
         */
        static void delete( Path path ) throws IOException {
            if ( path != null ) {
                if ( inTargetDir(path)) {
                    print("Deleting '" + path + "'...");
                    Set<FileVisitOption> options = EnumSet.noneOf(FileVisitOption.class);
                    int maxDepth = 10;
                    java.nio.file.Files.walkFileTree(path, options, maxDepth, new SimpleFileVisitor<Path>() {
                        @Override
                        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                            debug("Deleting '" + file.getFileName() + "':");
                            java.nio.file.Files.delete(file);
                            return FileVisitResult.SKIP_SUBTREE;
                        }
                        @Override
                        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                            debug("Deleting '" + dir.getFileName() + "':");
                            java.nio.file.Files.delete(dir);
                            return FileVisitResult.CONTINUE;
                        }
                        @Override
                        public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                            printError("Unable to remove '" + file.getFileName() + "'");
                            printError(exc);
                            return FileVisitResult.CONTINUE;
                        }
                    });
                } else {
                    System.out.println("Will not remove directory that is outside test target area: " + path);
                }
            }
        }
        
        static boolean inTargetDir( Path path ) {
            Path target = FileSystems.getDefault().getPath("target").toAbsolutePath();
            return path.toAbsolutePath().startsWith(target);
        }
    }
}
