/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import org.debezium.driver.EmbeddedDebezium;
import org.debezium.driver.EmbeddedDebeziumServices;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

/**
 * A set of integration tests for Debezium, using {@link EmbeddedDebezium} instances.
 * 
 * @author Randall Hauch
 */
public class EmbeddedDebeziumServicesTest {

    @Test
    public void shouldCreateEmbeddedDebeziumServices() {
        EmbeddedDebeziumServices services = new EmbeddedDebeziumServices();
        assertThat(services).isNotNull();
    }
}
