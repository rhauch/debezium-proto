/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.samza

import org.apache.samza.container.SamzaContainer
import org.apache.samza.job.ApplicationStatus
import org.apache.samza.job.ApplicationStatus.New
import org.apache.samza.job.ApplicationStatus.Running
import org.apache.samza.job.ApplicationStatus.SuccessfulFinish
import org.apache.samza.job.ApplicationStatus.UnsuccessfulFinish
import org.apache.samza.util.Logging

class SliceContainer(
  val containerId: Int, 
  private val container: Runnable) extends Logging {
  var thread: Thread = null
  @volatile var jobStatus: Option[ApplicationStatus] = None

  def start = {
    jobStatus = Some(New)
    thread = new Thread {
      override def run {
        try {
          container.run
          jobStatus = Some(SuccessfulFinish)
        } catch {
          case e: InterruptedException => {
            jobStatus = Some(UnsuccessfulFinish)
            Thread.interrupted
          }
          case e: Exception => {
            error("Failing job with exception.", e)
            jobStatus = Some(UnsuccessfulFinish)
            throw e
          }
        }
      }
    }
    thread.setName("SliceContainer" + containerId)
    thread.start
    jobStatus = Some(Running)
  }

  def stop = {
    thread.interrupt
  }

  def waitForFinish(timeoutMs: Long): ApplicationStatus = {
    thread.join(timeoutMs)
    getStatus
  }

  def getStatus = jobStatus.getOrElse(null)
}
