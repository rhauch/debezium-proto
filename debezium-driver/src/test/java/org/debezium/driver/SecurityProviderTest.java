/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.debezium.driver.SecurityProvider.Action;
import org.debezium.driver.SecurityProvider.CompositeAction;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

import static org.junit.Assert.fail;

/**
 * @author Randall Hauch
 *
 */
public class SecurityProviderTest {

    @Test
    public void shouldCorrectlyBuildCompositeActionFromSingleActionWithOnePrivilege() {
        checkProvider("db1:r");
    }

    @Test
    public void shouldCorrectlyBuildCompositeActionFromSingleActionWithTwoPrivileges() {
        checkProvider("db1:rw");
    }

    @Test
    public void shouldCorrectlyBuildCompositeActionFromSingleActionWithThreePrivileges() {
        checkProvider("db1:rwa");
    }

    @Test
    public void shouldCorrectlyBuildCompositeActionFromMultipleActions() {
        checkProvider("db1:r", "db2:r");
    }

    protected static void checkProvider(String... actionStrings) {
        Set<Action> expectedActions = new HashSet<>();
        for (String actionStr : actionStrings) {
            String[] parts = actionStr.split(":");
            Action action = new Action(parts[0]);
            if (parts[1].contains("r")) action = action.withRead();
            if (parts[1].contains("w")) action = action.withWrite();
            if (parts[1].contains("a")) action = action.withAdminister();
            if (parts[1].contains("p")) action = action.withProvision();
            if (parts[1].contains("d")) action = action.withDestroy();
            expectedActions.add(action);
        }
        SecurityProvider provider = new SecurityProvider() {
            @Override
            public SessionToken authenticate(String username, String device, String appVersion, Set<String> existingDbIds,
                                             String... databaseIds) {
                fail("Should not have been called");
                return null;
            }

            @Override
            public String check(SessionToken token, Iterator<Action> actions) {
                while (actions.hasNext()) {
                    Action action = actions.next();
                    assertThat(expectedActions.remove(action)).isTrue();
                }
                assertThat(expectedActions.isEmpty());
                return "success";
            }

            @Override
            public String getName() {
                fail("Should not have been called");
                return null;
            }

            @Override
            public void info(SessionToken token, SessionTokenAccessor accessor) {
                fail("Should not have been called");
            }

            @Override
            public void shutdown() {
                fail("Should not have been called");
            }
        };
        CompositeAction ca = provider.check(token());
        for (Action action : expectedActions) {
            if (action.canRead()) ca.canRead(action.databaseId());
            if (action.canWrite()) ca.canWrite(action.databaseId());
            if (action.canAdminister()) ca.canAdminister(action.databaseId());
            if (action.canProvision()) ca.canProvision(action.databaseId());
            if (action.canDestroy()) ca.canDestroy(action.databaseId());
        }
        assertThat(ca.submit()).isEqualTo("success");
    }

    @SuppressWarnings("serial")
    private static SessionToken token() {
        return new SessionToken() {
        };
    }
}
