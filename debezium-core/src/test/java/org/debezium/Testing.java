/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium;

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
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import org.debezium.core.util.IoUtil;
import org.debezium.core.util.Stopwatch;
import org.debezium.core.util.Stopwatch.Statistics;
import org.debezium.core.util.Stopwatch.StopwatchSet;
import org.fest.assertions.Fail;
import org.junit.Before;

import static org.fest.assertions.Assertions.assertThat;

import static org.fest.assertions.Fail.fail;

/**
 * A set of utility methods for test cases.
 * 
 * @author Randall Hauch
 */
public interface Testing {

    @Before
    default void resetBeforeEachTest() {
        Print.enabled = false;
        Debug.enabled = false;
        Timer.reset();
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

    public static void print(Object message) {
        if (message != null && Print.enabled) {
            System.out.println(message);
        }
    }

    public static void print(int length, String leader, Object message) {
        if (message != null && Print.enabled) {
            int len = leader.length();
            System.out.print(leader);
            if (len < length) for (int i = len; i != length; ++i)
                System.out.print(" ");
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

    public static void debug(Object message) {
        if (message != null && Debug.enabled) {
            System.out.println(message);
        }
    }

    public static void printError(Object message) {
        if (message != null) {
            System.err.println(message);
        }
    }

    public static void printError(Throwable throwable) {
        if (throwable != null) {
            throwable.printStackTrace();
        }
    }

    public static interface Files {

        public static InputStream readResourceAsStream(String pathOnClasspath) {
            InputStream stream = Testing.class.getClassLoader().getResourceAsStream(pathOnClasspath);
            assertThat(stream).isNotNull();
            return stream;
        }

        public static String readResourceAsString(String pathOnClasspath) {
            try (InputStream stream = readResourceAsStream(pathOnClasspath)) {
                return IoUtil.read(stream);
            } catch (IOException e) {
                Fail.fail("Unable to read '" + pathOnClasspath + "'", e);
                return null;
            }
        }

        /**
         * A method that will delete a file or folder only if it is within the 'target' directory (for safety).
         * Folders are removed recursively.
         * 
         * @param path the path to the file or folder in the target directory
         * @throws IOException if there is a problem deleting the files at this path
         */
        static void delete(Path path) throws IOException {
            if (path != null) {
                if (inTargetDir(path)) {
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

        static boolean inTargetDir(Path path) {
            Path target = FileSystems.getDefault().getPath("target").toAbsolutePath();
            return path.toAbsolutePath().startsWith(target);
        }
    }

    default public Statistics once(InterruptableFunction runnable) throws InterruptedException {
        return Timer.time(null, 1, runnable, null);
    }

    default public <T> Statistics once(Callable<T> runnable, Consumer<T> cleanup) throws InterruptedException {
        return Timer.time(null, 1, runnable, cleanup);
    }

    default public Statistics time(String desc, int repeat, InterruptableFunction runnable) throws InterruptedException {
        return Timer.time(desc, repeat, runnable, null);
    }

    default public <T> Statistics time(String desc, int repeat, Callable<T> runnable, Consumer<T> cleanup) throws InterruptedException {
        return Timer.time(desc, repeat, runnable, cleanup);
    }

    public static final class Timer {
        private static Stopwatch sw = Stopwatch.accumulating();
        private static StopwatchSet sws = Stopwatch.multiple();

        public static void reset() {
            sw = Stopwatch.accumulating();
            sws = Stopwatch.multiple();
        }

        public static Statistics completionTime() {
            return sw.durations().statistics();
        }

        public static Statistics operationTimes() {
            return sws.statistics();
        }

        protected static <T> Statistics time(String desc, int repeat, Callable<T> runnable, Consumer<T> cleanup)
                throws InterruptedException {
            sw.start();
            try {
                sws.time(repeat, runnable, result -> {
                    if (cleanup != null) cleanup.accept(result);
                });
            } catch (Throwable t) {
                t.printStackTrace();
                fail(t.getMessage());
            }
            sw.stop();
            // if (desc != null) Testing.print(60, "Time to " + desc + ":", sw.durations().statistics().getTotalAsString());
            // Testing.print(60,"Total clock time:",sw.durations().statistics().getTotalAsString());
            // Testing.print(54,"Time to invoke the functions:",sws);
            return sw.durations().statistics();
        }

    }

    @FunctionalInterface
    public static interface InterruptableFunction extends Callable<Void> {
        @Override
        public Void call() throws InterruptedException;
    }

}
