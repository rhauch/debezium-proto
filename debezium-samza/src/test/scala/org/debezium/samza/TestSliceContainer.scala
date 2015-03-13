/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.debezium.samza

import org.junit.Assert._
import org.junit.Test
import org.apache.samza.job.ApplicationStatus

class TestSliceContainer {
  @Test
  def testSliceContainerShouldFinishOnItsOwn {
    val job = new SliceContainer("jobX",new Runnable {
      override def run {
      }
    })
    job.start
    assertEquals(ApplicationStatus.SuccessfulFinish, job.waitForFinish(999999999))
  }

  @Test
  def testSliceContainerKillShouldWork {
    val job = new SliceContainer("jobX",new Runnable {
      override def run {
        Thread.sleep(999999)
      }
    })
    job.start
    job.waitForFinish(500)
    job.stop
    job.waitForFinish(999999)
    assertEquals(ApplicationStatus.UnsuccessfulFinish, job.waitForFinish(999999999))
  }
}