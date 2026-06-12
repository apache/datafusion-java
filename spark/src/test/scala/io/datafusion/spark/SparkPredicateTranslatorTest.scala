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

package io.datafusion.spark

import org.apache.datafusion.protobuf.LogicalExprNode
import org.apache.spark.sql.connector.expressions.{Expression, Expressions, NamedReference}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.scalatest.funsuite.AnyFunSuite

class SparkPredicateTranslatorTest extends AnyFunSuite {

  private def col(name: String): NamedReference = Expressions.column(name)
  private def litInt(v: Int): Expression = Expressions.literal(Int.box(v))
  private def litLong(v: Long): Expression = Expressions.literal(Long.box(v))
  private def litStr(v: String): Expression =
    Expressions.literal(org.apache.spark.unsafe.types.UTF8String.fromString(v))

  test("LessThan(timeline, 1_000_000) translates to a non-empty proto") {
    val p = new Predicate("<", Array[Expression](col("timeline"), litLong(1000000L)))
    val node = SparkPredicateTranslator.translate(p).getOrElse(fail("expected Some"))
    val bytes = node.toByteArray
    assert(bytes.nonEmpty)
    val parsed = LogicalExprNode.parseFrom(bytes)
    assert(parsed.hasBinaryExpr)
    assert(parsed.getBinaryExpr.getOp == "Lt")
  }

  test("AND of two translatable predicates round-trips through binary op 'And'") {
    val lt = new Predicate("<", Array[Expression](col("a"), litInt(10)))
    val eq = new Predicate("=", Array[Expression](col("b"), litStr("x")))
    val and = new Predicate("AND", Array[Expression](lt, eq))
    val node = SparkPredicateTranslator.translate(and).getOrElse(fail("expected Some"))
    val parsed = LogicalExprNode.parseFrom(node.toByteArray)
    assert(parsed.hasBinaryExpr)
    assert(parsed.getBinaryExpr.getOp == "And")
  }

  test("AND becomes residual when an operand is untranslatable") {
    val nse = new Predicate("<=>", Array[Expression](col("a"), litInt(1)))
    val eq = new Predicate("=", Array[Expression](col("b"), litInt(2)))
    val and = new Predicate("AND", Array[Expression](nse, eq))
    assert(SparkPredicateTranslator.translate(and).isEmpty)
  }

  test("IS_NULL and IS_NOT_NULL emit the dedicated proto variants") {
    val isNull = new Predicate("IS_NULL", Array[Expression](col("x")))
    val isNotNull = new Predicate("IS_NOT_NULL", Array[Expression](col("x")))
    val n1 = SparkPredicateTranslator.translate(isNull).getOrElse(fail()).toByteArray
    val n2 = SparkPredicateTranslator.translate(isNotNull).getOrElse(fail()).toByteArray
    val p1 = LogicalExprNode.parseFrom(n1)
    val p2 = LogicalExprNode.parseFrom(n2)
    assert(p1.hasIsNullExpr)
    assert(p2.hasIsNotNullExpr)
  }

  test("STARTS_WITH translates to a LIKE with a '%' suffix") {
    val p =
      new Predicate("STARTS_WITH", Array[Expression](col("name"), litStr("foo")))
    val node = SparkPredicateTranslator.translate(p).getOrElse(fail())
    val parsed = LogicalExprNode.parseFrom(node.toByteArray)
    assert(parsed.hasLike)
    val patStr = parsed.getLike.getPattern.getLiteral.getUtf8Value
    assert(patStr == "foo%")
  }

  test("unknown predicate name returns None (becomes residual)") {
    val p = new Predicate("UNKNOWN_OP", Array[Expression](col("x"), litInt(1)))
    assert(SparkPredicateTranslator.translate(p).isEmpty)
  }
}
