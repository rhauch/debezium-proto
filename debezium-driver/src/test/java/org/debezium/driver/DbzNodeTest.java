/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class DbzNodeTest extends AbstractDbzNodeTest {

    @Test
    public void shouldConsumeMessageThatWasPublished() throws InterruptedException {
        sendAndReceiveMessages(10, 1, "topicA", 10, TimeUnit.SECONDS);
    }

}
