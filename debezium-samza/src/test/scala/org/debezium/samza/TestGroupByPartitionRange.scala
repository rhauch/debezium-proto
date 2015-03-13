/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.debezium.samza

import org.apache.samza.container.TaskName
import org.apache.samza.container.grouper.stream.SystemStreamPartitionGrouper
import org.apache.samza.system.SystemStreamPartition
import org.junit.Test

import scala.collection.JavaConverters._

/*
class TestGroupByPartition {
  @Test def groupingWorks() {
    println("Grouping works!!")
  }
}
*/
class TestGroupByPartitionRange extends GroupByTestBase {
  import GroupByTestBase._
  
  var grouper: SystemStreamPartitionGrouper = GroupByPartitionRange.all

  override def getGrouper: SystemStreamPartitionGrouper = grouper

  def verifyPartitionsGroupedCorrectly(ranges: Traversable[String], expected: java.util.Map[TaskName, java.util.Set[SystemStreamPartition]]) {
    ranges.foreach { range =>
      grouper = GroupByPartitionRange.from(range)
      verifyGroupGroupsCorrectly(allSSPs, expected)
    }
  }

  @Test def groupingNoPartitionsWorks() {
    grouper = GroupByPartitionRange.none
    val expected = new java.util.HashMap[TaskName,java.util.Set[SystemStreamPartition]]
    verifyGroupGroupsCorrectly(allSSPs,expected)
  }
  
  @Test def groupingNoMatchingPartitionWorks() {
    val expected = new java.util.HashMap[TaskName,java.util.Set[SystemStreamPartition]]
    verifyPartitionsGroupedCorrectly("110"::" 6 "::"6:10000"::Nil,expected)
  }
  
  @Test def groupingAllPartitionsWorks() {
    var expected = Map(new TaskName("Partition 0") -> Set(aa0, ac0).asJava,
                       new TaskName("Partition 1") -> Set(aa1, ab1).asJava,
                       new TaskName("Partition 2") -> Set(aa2, ab2).asJava,
                       new TaskName("Partition 3") -> Set(aa3, ab3).asJava,
                       new TaskName("Partition 4") -> Set(aa4, ab4).asJava,
                       new TaskName("Partition 5") -> Set(ab5).asJava
                       ).asJava
    verifyPartitionsGroupedCorrectly("all"::"ALL"::"aLL"::" ALL "::Nil,expected)
  }
  
  @Test def groupingSinglePartitionZeroWorks() {
    val expected = Map(new TaskName("Partition 0") -> Set(aa0, ac0).asJava).asJava
    verifyPartitionsGroupedCorrectly("0"::" 0 "::"0:0"::"  0:0 "::Nil,expected)
  }
  
  @Test def groupingSingularRangePartitionOneWorks() {
    val expected = Map(new TaskName("Partition 1") -> Set(aa1, ab1).asJava).asJava
    verifyPartitionsGroupedCorrectly("1"::" 1 "::"1:1"::"  1:1 "::Nil,expected)
  }
  
  @Test def groupingSingularRangePartitionTwoWorks() {
    val expected = Map(new TaskName("Partition 2") -> Set(aa2, ab2).asJava).asJava
    verifyPartitionsGroupedCorrectly("2"::" 2 "::"2:2"::"  2:2 "::Nil,expected)
  }
  
  @Test def groupingSingularRangePartitionThreeWorks() {
    val expected = Map(new TaskName("Partition 3") -> Set(aa3, ab3).asJava).asJava
    verifyPartitionsGroupedCorrectly("3"::" 3 "::"3:3"::"  3:3 "::Nil,expected)
  }
  
  @Test def groupingSingularRangePartitionFiveWorks() {
    val expected = Map(new TaskName("Partition 5") -> Set(ab5).asJava).asJava
    verifyPartitionsGroupedCorrectly("5"::" 5 "::"5:5"::"  5:5 "::Nil,expected)
  }
  
  @Test def groupingMultiplePartitionsWorks() {
    var expected = Map(new TaskName("Partition 1") -> Set(aa1, ab1).asJava,
                       new TaskName("Partition 2") -> Set(aa2, ab2).asJava,
                       new TaskName("Partition 3") -> Set(aa3, ab3).asJava
                       ).asJava
    verifyPartitionsGroupedCorrectly("1:3"::" 1:3 "::Nil,expected)
  }
  
  @Test def groupingMultiplePartitionsWithBeyondUpperLimitWorks() {
    var expected = Map(new TaskName("Partition 1") -> Set(aa1, ab1).asJava,
                       new TaskName("Partition 2") -> Set(aa2, ab2).asJava,
                       new TaskName("Partition 3") -> Set(aa3, ab3).asJava,
                       new TaskName("Partition 4") -> Set(aa4, ab4).asJava,
                       new TaskName("Partition 5") -> Set(ab5).asJava
                       ).asJava
    verifyPartitionsGroupedCorrectly("1:300"::" 1:300 "::Nil,expected)
  }
  
}
