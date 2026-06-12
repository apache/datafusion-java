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

import datafusion_common.DatafusionCommon.{Column, ScalarValue}
import org.apache.datafusion.protobuf.{
  BinaryExprNode,
  InListNode,
  IsNotNull,
  IsNull,
  LikeNode,
  LogicalExprNode,
  Not => NotNode
}
import org.apache.spark.sql.connector.expressions.{Expression, Literal, NamedReference}
import org.apache.spark.sql.connector.expressions.filter.Predicate

/**
 * Translate Spark V2 `Predicate` → DataFusion `LogicalExprNode` proto. Only emits expressions
 * that the producer can apply EXACTLY — anything else returns `None` and the caller marks the
 * predicate as residual so Spark re-applies it above the scan.
 */
object SparkPredicateTranslator {

  def translate(p: Predicate): Option[LogicalExprNode] = p.name() match {
    case "=" => binary(p, "Eq")
    case "<>" => binary(p, "NotEq")
    case "<" => binary(p, "Lt")
    case "<=" => binary(p, "LtEq")
    case ">" => binary(p, "Gt")
    case ">=" => binary(p, "GtEq")
    case "IS_NULL" => unary(p, "IsNull")
    case "IS_NOT_NULL" => unary(p, "IsNotNull")
    case "AND" => combine(p, "And")
    case "OR" => combine(p, "Or")
    case "NOT" => translateNot(p)
    case "IN" => translateIn(p)
    case "STARTS_WITH" => like(p, prefix = false, suffix = true)
    case "ENDS_WITH" => like(p, prefix = true, suffix = false)
    case "CONTAINS" => like(p, prefix = true, suffix = true)
    case _ => None
  }

  private def binary(p: Predicate, op: String): Option[LogicalExprNode] = {
    val cs = p.children()
    if (cs.length != 2) return None
    val left = expr(cs(0))
    val right = expr(cs(1))
    if (left.isEmpty || right.isEmpty) return None
    Some(
      LogicalExprNode
        .newBuilder()
        .setBinaryExpr(
          BinaryExprNode
            .newBuilder()
            .addOperands(left.get)
            .addOperands(right.get)
            .setOp(op)
            .build()
        )
        .build()
    )
  }

  private def unary(p: Predicate, op: String): Option[LogicalExprNode] = {
    val cs = p.children()
    if (cs.length != 1) return None
    val inner = expr(cs(0))
    if (inner.isEmpty) return None
    val builder = LogicalExprNode.newBuilder()
    op match {
      case "IsNull" => builder.setIsNullExpr(IsNull.newBuilder().setExpr(inner.get).build())
      case "IsNotNull" =>
        builder.setIsNotNullExpr(IsNotNull.newBuilder().setExpr(inner.get).build())
      case _ => return None
    }
    Some(builder.build())
  }

  private def combine(p: Predicate, op: String): Option[LogicalExprNode] = {
    val cs = p.children()
    if (cs.length != 2) return None
    val (l, r) = (cs(0), cs(1))
    if (!l.isInstanceOf[Predicate] || !r.isInstanceOf[Predicate]) return None
    val ln = translate(l.asInstanceOf[Predicate])
    val rn = translate(r.asInstanceOf[Predicate])
    if (ln.isEmpty || rn.isEmpty) return None
    Some(
      LogicalExprNode
        .newBuilder()
        .setBinaryExpr(
          BinaryExprNode
            .newBuilder()
            .addOperands(ln.get)
            .addOperands(rn.get)
            .setOp(op)
            .build()
        )
        .build()
    )
  }

  private def translateNot(p: Predicate): Option[LogicalExprNode] = {
    val cs = p.children()
    if (cs.length != 1 || !cs(0).isInstanceOf[Predicate]) return None
    val inner = translate(cs(0).asInstanceOf[Predicate])
    if (inner.isEmpty) return None
    Some(LogicalExprNode.newBuilder().setNotExpr(NotNode.newBuilder().setExpr(inner.get).build()).build())
  }

  private def translateIn(p: Predicate): Option[LogicalExprNode] = {
    val cs = p.children()
    if (cs.length < 2) return None
    val target = expr(cs(0))
    if (target.isEmpty) return None
    val values = new java.util.ArrayList[LogicalExprNode]()
    var i = 1
    while (i < cs.length) {
      val v = expr(cs(i))
      if (v.isEmpty) return None
      values.add(v.get)
      i += 1
    }
    val node = InListNode
      .newBuilder()
      .setExpr(target.get)
      .addAllList(values)
      .setNegated(false)
      .build()
    Some(LogicalExprNode.newBuilder().setInList(node).build())
  }

  private def like(p: Predicate, prefix: Boolean, suffix: Boolean): Option[LogicalExprNode] = {
    val cs = p.children()
    if (cs.length != 2) return None
    val target = expr(cs(0))
    val pat = cs(1) match {
      case lit: Literal[_] =>
        val raw = lit.value() match {
          case s: String => Some(s)
          case u: org.apache.spark.unsafe.types.UTF8String => Some(u.toString)
          case _ => None
        }
        raw.map { r =>
          val escaped = r.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
          (if (prefix) "%" else "") + escaped + (if (suffix) "%" else "")
        }
      case _ => None
    }
    if (target.isEmpty || pat.isEmpty) return None
    val patternExpr = stringLiteral(pat.get)
    val like = LikeNode
      .newBuilder()
      .setExpr(target.get)
      .setPattern(patternExpr)
      .setNegated(false)
      .setEscapeChar("\\")
      .build()
    Some(LogicalExprNode.newBuilder().setLike(like).build())
  }

  private def expr(e: Expression): Option[LogicalExprNode] = e match {
    case nr: NamedReference =>
      val parts = nr.fieldNames()
      if (parts.length != 1) None
      else
        Some(
          LogicalExprNode
            .newBuilder()
            .setColumn(Column.newBuilder().setName(parts(0)).build())
            .build()
        )
    case lit: Literal[_] => literal(lit.value())
    case _ => None
  }

  private def literal(v: Any): Option[LogicalExprNode] = {
    val sv = ScalarValue.newBuilder()
    val ok: Boolean = v match {
      case b: java.lang.Boolean => sv.setBoolValue(b.booleanValue()); true
      case b: java.lang.Byte => sv.setInt8Value(b.intValue()); true
      case s: java.lang.Short => sv.setInt16Value(s.intValue()); true
      case i: java.lang.Integer => sv.setInt32Value(i.intValue()); true
      case l: java.lang.Long => sv.setInt64Value(l.longValue()); true
      case f: java.lang.Float => sv.setFloat32Value(f.floatValue()); true
      case d: java.lang.Double => sv.setFloat64Value(d.doubleValue()); true
      case s: String => sv.setUtf8Value(s); true
      case u: org.apache.spark.unsafe.types.UTF8String => sv.setUtf8Value(u.toString); true
      case _ => false
    }
    if (!ok) None
    else Some(LogicalExprNode.newBuilder().setLiteral(sv.build()).build())
  }

  private def stringLiteral(s: String): LogicalExprNode =
    LogicalExprNode.newBuilder().setLiteral(ScalarValue.newBuilder().setUtf8Value(s).build()).build()
}
