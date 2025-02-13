/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Alias, Rand}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.types.MetadataBuilder

class CollapseProjectSuite extends PlanTest {
  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Subqueries", FixedPoint(10), EliminateSubqueryAliases) ::
      Batch("CollapseProject", Once, CollapseProject) ::
      Batch("SimplifyExtractValueOps", Once, SimplifyExtractValueOps) :: Nil
  }

  val testRelation = LocalRelation($"a".int, $"b".int)

  test("collapse two deterministic, independent projects into one") {
    val query = testRelation
      .select(($"a" + 1).as(Symbol("a_plus_1")), $"b")
      .select($"a_plus_1", ($"b" + 1).as(Symbol("b_plus_1")))

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = testRelation.select(($"a" + 1).as(Symbol("a_plus_1")),
      ($"b" + 1).as(Symbol("b_plus_1"))).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("collapse two deterministic, dependent projects into one") {
    val query = testRelation
      .select(($"a" + 1).as(Symbol("a_plus_1")), $"b")
      .select(($"a_plus_1" + 1).as(Symbol("a_plus_2")), $"b")

    val optimized = Optimize.execute(query.analyze)

    val correctAnswer = testRelation.select(
      (($"a" + 1).as(Symbol("a_plus_1")) + 1).as(Symbol("a_plus_2")),
      $"b").analyze

    comparePlans(optimized, correctAnswer)
  }

  test("do not collapse nondeterministic projects") {
    val query = testRelation
      .select(Rand(10).as(Symbol("rand")))
      .select(($"rand" + 1).as(Symbol("rand1")), ($"rand" + 2).as(Symbol("rand2")))

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = query.analyze

    comparePlans(optimized, correctAnswer)
  }

  test("collapse two nondeterministic, independent projects into one") {
    val query = testRelation
      .select(Rand(10).as(Symbol("rand")))
      .select(Rand(20).as(Symbol("rand2")))

    val optimized = Optimize.execute(query.analyze)

    val correctAnswer = testRelation
      .select(Rand(20).as(Symbol("rand2"))).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("collapse one nondeterministic, one deterministic, independent projects into one") {
    val query = testRelation
      .select(Rand(10).as(Symbol("rand")), $"a")
      .select(($"a" + 1).as(Symbol("a_plus_1")))

    val optimized = Optimize.execute(query.analyze)

    val correctAnswer = testRelation
      .select(($"a" + 1).as(Symbol("a_plus_1"))).analyze

    comparePlans(optimized, correctAnswer)
  }

  test("collapse project into aggregate") {
    val query = testRelation
      .groupBy($"a", $"b")(($"a" + 1).as(Symbol("a_plus_1")), $"b")
      .select($"a_plus_1", ($"b" + 1).as(Symbol("b_plus_1")))

    val optimized = Optimize.execute(query.analyze)

    val correctAnswer = testRelation
      .groupBy($"a", $"b")(($"a" + 1).as(Symbol("a_plus_1")), ($"b" + 1).as(Symbol("b_plus_1")))
      .analyze

    comparePlans(optimized, correctAnswer)
  }

  test("do not collapse common nondeterministic project and aggregate") {
    val query = testRelation
      .groupBy($"a")($"a", Rand(10).as(Symbol("rand")))
      .select(($"rand" + 1).as(Symbol("rand1")), ($"rand" + 2).as(Symbol("rand2")))

    val optimized = Optimize.execute(query.analyze)
    val correctAnswer = query.analyze

    comparePlans(optimized, correctAnswer)
  }

  test("SPARK-36718: do not collapse project if non-cheap expressions will be repeated") {
    val query = testRelation
      .select(($"a" + 1).as("a_plus_1"))
      .select(($"a_plus_1" + $"a_plus_1").as("a_2_plus_2"))
      .analyze

    val optimized = Optimize.execute(query)
    comparePlans(optimized, query)

    // CreateStruct is an exception if it's only referenced by ExtractValue.
    val query2 = testRelation
      .select(namedStruct("a", $"a", "a_plus_1", $"a" + 1).as("struct"))
      .select(($"struct".getField("a") + $"struct".getField("a_plus_1")).as("add"))
      .analyze
    val optimized2 = Optimize.execute(query2)
    val expected2 = testRelation
      .select(($"a" + ($"a" + 1)).as("add"))
      .analyze
    comparePlans(optimized2, expected2)
  }

  test("preserve top-level alias metadata while collapsing projects") {
    def hasMetadata(logicalPlan: LogicalPlan): Boolean = {
      logicalPlan.asInstanceOf[Project].projectList.exists(_.metadata.contains("key"))
    }

    val metadata = new MetadataBuilder().putLong("key", 1).build()
    val analyzed =
      Project(Seq(Alias($"a_with_metadata", "b")()),
        Project(Seq(Alias($"a", "a_with_metadata")(explicitMetadata = Some(metadata))),
          testRelation.logicalPlan)).analyze
    require(hasMetadata(analyzed))

    val optimized = Optimize.execute(analyzed)
    val projects = optimized.collect { case p: Project => p }
    assert(projects.size === 1)
    assert(hasMetadata(optimized))
  }

  test("collapse redundant alias through limit") {
    val relation = LocalRelation($"a".int, $"b".int)
    val query = relation.select($"a" as Symbol("b")).limit(1).select($"b" as Symbol("c")).analyze
    val optimized = Optimize.execute(query)
    val expected = relation.select($"a" as Symbol("c")).limit(1).analyze
    comparePlans(optimized, expected)
  }

  test("collapse redundant alias through local limit") {
    val relation = LocalRelation($"a".int, $"b".int)
    val query = LocalLimit(1, relation.select($"a" as Symbol("b")))
      .select($"b" as Symbol("c")).analyze
    val optimized = Optimize.execute(query)
    val expected = LocalLimit(1, relation.select($"a" as Symbol("c"))).analyze
    comparePlans(optimized, expected)
  }

  test("collapse redundant alias through repartition") {
    val relation = LocalRelation($"a".int, $"b".int)
    val query = relation.select($"a" as Symbol("b")).repartition(1)
      .select($"b" as Symbol("c")).analyze
    val optimized = Optimize.execute(query)
    val expected = relation.select($"a" as Symbol("c")).repartition(1).analyze
    comparePlans(optimized, expected)
  }

  test("collapse redundant alias through sample") {
    val relation = LocalRelation($"a".int, $"b".int)
    val query = Sample(0.0, 0.6, false, 11L, relation.select($"a" as Symbol("b")))
      .select($"b" as Symbol("c")).analyze
    val optimized = Optimize.execute(query)
    val expected = Sample(0.0, 0.6, false, 11L, relation.select($"a" as Symbol("c"))).analyze
    comparePlans(optimized, expected)
  }

  test("SPARK-36086: CollapseProject should keep output schema name") {
    val relation = LocalRelation($"a".int, $"b".int)
    val select = relation.select(($"a" + $"b").as(Symbol("c"))).analyze
    val query = Project(Seq(select.output.head.withName("C")), select)
    val optimized = Optimize.execute(query)
    val expected = relation.select(($"a" + $"b").as(Symbol("C"))).analyze
    comparePlans(optimized, expected)
  }
}
