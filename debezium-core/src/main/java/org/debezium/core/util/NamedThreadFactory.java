/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.core.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * A simple named thread factory that creates threads named "{@code $PREFIX$-$NAME$-thread-$NUMBER$}".
 * @author Randall Hauch
 */
public final class NamedThreadFactory implements ThreadFactory {
    
    private static final boolean DEFAULT_DAEMON_THREAD = true;
    private static final int DEFAULT_STACK_SIZE = 0;
    
    private final boolean daemonThreads;
    private final ThreadGroup group;
    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final String namePrefix;
    private final int stackSize;
    private final Consumer<String> afterThreadCreation;

    public NamedThreadFactory(String prefix, String name) {
        this(prefix,name,DEFAULT_DAEMON_THREAD,DEFAULT_STACK_SIZE);
    }

    public NamedThreadFactory(String prefix, String name, boolean daemonThreads) {
        this(prefix,name,daemonThreads,DEFAULT_STACK_SIZE);
    }

    public NamedThreadFactory(String prefix, String name, boolean daemonThreads, int stackSize) {
        this(prefix,name,daemonThreads,stackSize,null);
    }

    public NamedThreadFactory(String prefix, String name, boolean daemonThreads, int stackSize, Consumer<String> afterThreadCreation ) {
        final SecurityManager s = System.getSecurityManager();
        this.group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
        this.namePrefix = prefix + "-" + name + "-thread-";
        this.daemonThreads = daemonThreads;
        this.stackSize = stackSize;
        this.afterThreadCreation = afterThreadCreation;
    }

    @Override
    public Thread newThread(final Runnable runnable) {
        String threadName = namePrefix + threadNumber.getAndIncrement();
        final Thread t = new Thread(group, runnable, threadName, stackSize);
        t.setDaemon(daemonThreads);
        if (t.getPriority() != Thread.NORM_PRIORITY) {
            t.setPriority(Thread.NORM_PRIORITY);
        }
        if ( afterThreadCreation != null ) {
            try {
                afterThreadCreation.accept(threadName);
            } catch ( Throwable e ) {
                // do nothing
            }
        }
        return t;
    }
}

