/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.fest.assertions.Assertions.assertThat;

/**
 * @author Randall Hauch
 *
 */
public class MockSecurityProvider implements SecurityProvider {
    
    private final SecurityProvider delegate;
    private final Set<SessionToken> activeSessions = Collections.newSetFromMap(new ConcurrentHashMap<SessionToken,Boolean>());
    private final AtomicLong authenticated = new AtomicLong();
    private final AtomicLong checked = new AtomicLong();

    public MockSecurityProvider() {
        this(new PassthroughSecurityProvider());
    }
    
    public MockSecurityProvider( SecurityProvider delegate) {
        this.delegate = delegate;
    }
    
    @Override
    public SessionToken authenticate(String username, String device, String appVersion, Set<String> existingDatabaseIds,
                                     String... databaseIds) {
        SessionToken token = delegate.authenticate(username, device, appVersion, existingDatabaseIds, databaseIds);
        authenticated.incrementAndGet();
        assertThat(activeSessions.add(token)).isTrue();
        return token;
    }
    
    @Override
    public String check(SessionToken token, Iterator<Action> actions) {
        String username = delegate.check(token, actions);
        checked.incrementAndGet();
        return username;
    }
    
    @Override
    public String getName() {
        return delegate.getName();
    }
    
    @Override
    public void info(SessionToken token, SessionTokenAccessor accessor) {
        delegate.info(token, accessor);
    }
    
    @Override
    public void shutdown() {
        delegate.shutdown();
    }
    
    public void assertSessionCount( int expected ) {
        assertThat(activeSessions.size()).isEqualTo(expected);
    }

}
