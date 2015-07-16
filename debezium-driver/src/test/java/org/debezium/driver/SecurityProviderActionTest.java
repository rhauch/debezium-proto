/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.driver;

import org.debezium.driver.SecurityProvider.Action;
import org.junit.Test;

import static org.fest.assertions.Assertions.assertThat;

public class SecurityProviderActionTest {

    @Test
    public void shouldConsiderEqualTwoObjectsWithSameDatabaseId() {
        Action a1 = new Action("db1").withRead().withWrite();
        Action a2 = new Action("db1").withProvision().withAdminister().withDestroy();
        assertThat(a1).isEqualTo(a2);
    }

    @Test
    public void shouldConsiderNotEqualTwoObjectsWithDifferentDatabaseIds() {
        Action a1 = new Action("db1").withRead().withWrite();
        Action a2 = new Action("db2").withRead().withWrite();
        assertThat(a1).isNotEqualTo(a2);
    }

    @Test
    public void shouldCompareTwoObjectsWithSameDatabaseIdAndSameActions() {
        Action a1 = new Action("db1").withRead().withWrite();
        Action a2 = new Action("db1").withRead().withWrite();
        assertThat(a1.compareTo(a2)).isEqualTo(0);
    }

    @Test
    public void shouldOrderByDatabaseIdAndThenActions() {
        Action[] actions = new Action[32];
        actions[0] = new Action("db1");
        actions[1] = new Action("db1").withRead();
        actions[2] = new Action("db1").withWrite();
        actions[3] = new Action("db1").withWrite().withRead();
        actions[4] = new Action("db1").withAdminister();
        actions[5] = new Action("db1").withAdminister().withRead();
        actions[6] = new Action("db1").withAdminister().withWrite();
        actions[7] = new Action("db1").withAdminister().withWrite().withRead();
        actions[8] = new Action("db1").withProvision();
        actions[9] = new Action("db1").withProvision().withRead();
        actions[10] = new Action("db1").withProvision().withWrite();
        actions[11] = new Action("db1").withProvision().withWrite().withRead();
        actions[12] = new Action("db1").withProvision().withAdminister();
        actions[13] = new Action("db1").withProvision().withAdminister().withRead();
        actions[14] = new Action("db1").withProvision().withAdminister().withWrite();
        actions[15] = new Action("db1").withProvision().withAdminister().withWrite().withRead();
        actions[16] = new Action("db1").withDestroy();
        actions[17] = new Action("db1").withDestroy().withRead();
        actions[18] = new Action("db1").withDestroy().withWrite();
        actions[19] = new Action("db1").withDestroy().withWrite().withRead();
        actions[20] = new Action("db1").withDestroy().withAdminister();
        actions[21] = new Action("db1").withDestroy().withAdminister().withRead();
        actions[22] = new Action("db1").withDestroy().withAdminister().withWrite();
        actions[23] = new Action("db1").withDestroy().withAdminister().withWrite().withRead();
        actions[24] = new Action("db1").withDestroy().withProvision();
        actions[25] = new Action("db1").withDestroy().withProvision().withRead();
        actions[26] = new Action("db1").withDestroy().withProvision().withWrite();
        actions[27] = new Action("db1").withDestroy().withProvision().withWrite().withRead();
        actions[28] = new Action("db1").withDestroy().withProvision().withAdminister();
        actions[29] = new Action("db1").withDestroy().withProvision().withAdminister().withRead();
        actions[30] = new Action("db1").withDestroy().withProvision().withAdminister().withWrite();
        actions[31] = new Action("db1").withDestroy().withProvision().withAdminister().withWrite().withRead();
        for ( int i=0; i!=actions.length; ++i) {
            Action a1 = actions[i];
            for ( int j=0; j!=actions.length; ++j ) {
                Action a2 = actions[j];
                if ( i < j ) assertThat(a1.compareTo(a2)).isLessThan(0);
                else if ( i == j ) assertThat(a1.compareTo(a2)).isEqualTo(0);
                else /* (i > j) */ assertThat(a1.compareTo(a2)).isGreaterThan(0);
            }
        }
    }

}
