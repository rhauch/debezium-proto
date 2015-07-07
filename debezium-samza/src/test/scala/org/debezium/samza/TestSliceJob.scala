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

class TestSliceJob {
  @Test
  def testSliceJobWithOneContainerShouldFinishOnItsOwn {
    val container = new SliceContainer(1,new Runnable {
      override def run {
      }
    })
    val job = new SliceJob(container::Nil)
    job.submit
    assertEquals(ApplicationStatus.SuccessfulFinish, job.waitForFinish(999999999))
  }

  @Test
  def testSliceJobKillShouldWork {
    val container = new SliceContainer(1,new Runnable {
      override def run {
        Thread.sleep(999999)
      }
    })
    val job = new SliceJob(container::Nil)
    assertEquals(null,job.getStatus)
    job.submit
    job.waitForFinish(500)
    job.kill
    job.waitForFinish(999999)
    assertEquals(ApplicationStatus.UnsuccessfulFinish, job.waitForFinish(999999999))
  }
  
  @Test
  def testSliceJobWithTwoContainersShouldHaveCorrectStatus {
    val container1 = new SliceContainer(1,new Runnable {
      override def run {
      }
    })
    val container2 = new SliceContainer(1,new Runnable {
      override def run {
        Thread.sleep(999999)
      }
    })
    val job = new SliceJob(container1::container2::Nil)
    assertEquals(null,job.getStatus)
    job.submit
    assertEquals(ApplicationStatus.Running, job.getStatus)
    job.waitForFinish(500)
    assertEquals(ApplicationStatus.Running, job.getStatus)
    job.kill
    job.waitForFinish(999999)
    assertEquals(ApplicationStatus.UnsuccessfulFinish, job.waitForFinish(999999999))
  }

}