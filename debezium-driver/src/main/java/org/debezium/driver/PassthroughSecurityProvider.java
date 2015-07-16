/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.util.Iterator;
import java.util.Set;

import org.debezium.core.util.Collect;

/**
 * A pass-through security provider.
 * @author Randall Hauch
 */
final class PassthroughSecurityProvider implements SecurityProvider {
    
    private static interface Token {
        public String includesDatabase( String dbId );
        public void info( SessionTokenAccessor accessor );
        public String username();
    }
    
    
    private static final Token NON_MATCHING = new Token() {
        @Override
        public String includesDatabase(String dbId) {
            return null;
        }
        @Override
        public void info(SessionTokenAccessor accessor) {
        }
        @Override
        public String username() {
            return null;
        }
    };
    
    private static final class SingleDatabaseToken implements SessionToken, Token {
        private static final long serialVersionUID = 1L;
        private final String username;
        private final String device;
        private final String appVersion;
        private final String dbId;
        public SingleDatabaseToken( String username, String device, String appVersion, String dbId ) {
            this.username = username;
            this.device = device;
            this.appVersion = appVersion;
            this.dbId = dbId;
        }
        @Override
        public String includesDatabase(String dbId) {
            return this.dbId.equals(dbId) ? username : null;
        }
        @Override
        public void info(SessionTokenAccessor accessor) {
            accessor.record(username, device, appVersion);
        }
        @Override
        public String username() {
            return username;
        }
    }
    
    private static final class DualDatabaseToken implements SessionToken, Token {
        private static final long serialVersionUID = 1L;
        private final String username;
        private final String device;
        private final String appVersion;
        private final String dbIdA;
        private final String dbIdB;
        public DualDatabaseToken( String username, String device, String appVersion, String dbIdA, String dbIdB ) {
            this.username = username;
            this.device = device;
            this.appVersion = appVersion;
            this.dbIdA = dbIdA;
            this.dbIdB = dbIdB;
        }
        @Override
        public String includesDatabase(String dbId) {
            return (this.dbIdA.equals(dbId) || this.dbIdB.equals(dbId)) ? username : null;
        }
        @Override
        public void info(SessionTokenAccessor accessor) {
            accessor.record(username, device, appVersion);
        }
        @Override
        public String username() {
            return username;
        }
    }
    
    private static final class MultiDatabaseToken implements SessionToken, Token {
        private static final long serialVersionUID = 1L;
        private final String username;
        private final String device;
        private final String appVersion;
        private final Set<String> dbIds;
        public MultiDatabaseToken( String username, String device, String appVersion, Set<String> dbIds ) {
            this.username = username;
            this.device = device;
            this.appVersion = appVersion;
            this.dbIds = dbIds;
        }
        @Override
        public String includesDatabase(String dbId) {
            return dbIds.contains(dbId) ? username : null;
        }
        @Override
        public void info(SessionTokenAccessor accessor) {
            accessor.record(username, device, appVersion);
        }
        @Override
        public String username() {
            return username;
        }
    }

    public PassthroughSecurityProvider() {
    }

    @Override
    public String getName() {
        return "passthrough";
    }

    @Override
    public SessionToken authenticate(String username, String device, String appVersion, Set<String> existingDatabaseIds,
                                     String... databaseIds) {
        if ( databaseIds == null ) return null;
        final int numDbs = databaseIds.length;
        if ( numDbs == 1 && databaseIds[0] != null ) {
            return new SingleDatabaseToken(username, device, appVersion, databaseIds[0]);
        }
        if ( numDbs > 1 ) {
            Set<String> dbIds = Collect.unmodifiableSet(databaseIds);
            final int size = dbIds.size();
            if (size == 1 ) {
                return new SingleDatabaseToken(username, device, appVersion, dbIds.stream().findFirst().get());
            } else if (size == 2 ) {
                Iterator<String> iter = dbIds.iterator();
                return new DualDatabaseToken(username, device, appVersion, iter.next(), iter.next());
            } else if ( size > 2 ) {
                return new MultiDatabaseToken(username, device, appVersion, dbIds);
            }
        }
        return null;
    }
    
    private Token asDatabaseSet( SessionToken token ) {
        if ( token instanceof Token ) {
            return (Token)token;
        }
        return NON_MATCHING;
    }
    
    @Override
    public String check(SessionToken token, Iterator<Action> actions) {
        Token t = asDatabaseSet(token);
        String username = null;
        while ( actions.hasNext() ) {
            Action next = actions.next();
            username = t.includesDatabase(next.databaseId());
            if ( username == null ) return null;
        }
        return username;
    }
    
    @Override
    public String check(SessionToken token, Action action) {
        return asDatabaseSet(token).includesDatabase(action.databaseId());
    }
    
    @Override
    public void info(SessionToken token, SessionTokenAccessor accessor) {
        asDatabaseSet(token).info(accessor);
    }

    @Override
    public void shutdown() {
    }

}
