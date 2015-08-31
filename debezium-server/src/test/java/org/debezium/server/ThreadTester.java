/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.server;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Randall Hauch
 *
 */
public class ThreadTester {

    private static final Logger log = LoggerFactory.getLogger(ThreadTester.class);

    private static class MyThread extends Thread {
        
        private volatile boolean running = true;
        private final CountDownLatch latch = new CountDownLatch(1);
        public MyThread() {
            super();
        }

        /**
         * Execute the stream processors
         */
        @Override
        public void run() {
            log.info("Starting a kstream thread");
            try {
                runLoop();
            } catch (RuntimeException e) {
                log.error("Uncaught error during processing: ", e);
                throw e;
            } finally {
                shutdown();
            }
        }

        private void shutdown() {
            log.info("Shutting down a kstream thread");
            log.info("kstream thread shutdown complete");
        }

        /**
         * Shutdown this streaming thread.
         */
        public void close() {
            running = false;
        }

        private void runLoop() {
            log.info("entering the kstream thread runloop");
            int x = 0;
            while (stillRunning()) {
                // Do work ...
                ++x;
            }
            latch.countDown();
            log.info("exiting the kstream thread runloop");
        }

        private boolean stillRunning() {
            if (!running) {
                log.debug("Shutting down at user request.");
                return false;
            }
            return true;
        }
        
        private void await( long timeout, TimeUnit unit ) throws InterruptedException {
            latch.await(timeout,unit);
        }
    }


    @Test
    public void shouldStopThreadFromOriginatingThread() throws InterruptedException {

        // Create and start the main thread ...
        final MyThread thread = new MyThread();
        thread.start();

        log.info("Waiting 3 seconds before closing thread ...");
        Thread.sleep(3000);
        log.info("Closing thread");
        thread.close();
        
        log.info("Waiting for thread to complete ...");
        thread.await(15, TimeUnit.SECONDS);
        log.info("FINISHED!");
    }

    @Test
    public void shouldStopThreadFromSeparateThread() throws InterruptedException {
        final MyThread thread = new MyThread();

        // Start a new thread that will shut down the main thread after 3 seconds ...
        new Thread(() -> {
            try {
                Thread.sleep(3000);
                thread.close();
            } catch (InterruptedException e) {
                Thread.interrupted();
            }
        }).start();

        // Then start the main thread ...
        thread.start();

        log.info("Waiting for thread to complete ...");
        thread.await(15, TimeUnit.SECONDS);
        log.info("FINISHED!");
    }

}
