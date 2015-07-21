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

package org.apache.spark.sql.catalyst.expressions

import scala.collection.immutable.HashSet

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.types.{Decimal, DoubleType, IntegerType, BooleanType}


class PredicateSuite extends SparkFunSuite with ExpressionEvalHelper {

  private def booleanLogicTest(
    name: String,
    op: (Expression, Expression) => Expression,
    truthTable: Seq[(Any, Any, Any)]) {
    test(s"3VL $name") {
      truthTable.foreach {
        case (l, r, answer) =>
          val expr = op(Literal.create(l, BooleanType), Literal.create(r, BooleanType))
          checkEvaluation(expr, answer)
      }
    }
  }

  // scalastyle:off
  /**
   * Checks for three-valued-logic.  Based on:
   * http://en.wikipedia.org/wiki/Null_(SQL)#Comparisons_with_NULL_and_the_three-valued_logic_.283VL.29
   * I.e. in flat cpo "False -> Unknown -> True",
   *   OR is lowest upper bound,
   *   AND is greatest lower bound.
   * p       q       p OR q  p AND q  p = q
   * True    True    True    True     True
   * True    False   True    False    False
   * True    Unknown True    Unknown  Unknown
   * False   True    True    False    False
   * False   False   False   False    True
   * False   Unknown Unknown False    Unknown
   * Unknown True    True    Unknown  Unknown
   * Unknown False   Unknown False    Unknown
   * Unknown Unknown Unknown Unknown  Unknown
   *
   * p       NOT p
   * True    False
   * False   True
   * Unknown Unknown
   */
  // scalastyle:on

  test("3VL Not") {
    val notTrueTable =
      (true, false) ::
        (false, true) ::
        (null, null) :: Nil
    notTrueTable.foreach { case (v, answer) =>
      checkEvaluation(Not(Literal.create(v, BooleanType)), answer)
    }
  }

  booleanLogicTest("AND", And,
    (true, true, true) ::
      (true, false, false) ::
      (true, null, null) ::
      (false, true, false) ::
      (false, false, false) ::
      (false, null, false) ::
      (null, true, null) ::
      (null, false, false) ::
      (null, null, null) :: Nil)

  booleanLogicTest("OR", Or,
    (true, true, true) ::
      (true, false, true) ::
      (true, null, true) ::
      (false, true, true) ::
      (false, false, false) ::
      (false, null, null) ::
      (null, true, true) ::
      (null, false, null) ::
      (null, null, null) :: Nil)

  booleanLogicTest("=", EqualTo,
    (true, true, true) ::
      (true, false, false) ::
      (true, null, null) ::
      (false, true, false) ::
      (false, false, true) ::
      (false, null, null) ::
      (null, true, null) ::
      (null, false, null) ::
      (null, null, null) :: Nil)

  test("IN") {
    checkEvaluation(In(Literal(1), Seq(Literal(1), Literal(2))), true)
    checkEvaluation(In(Literal(2), Seq(Literal(1), Literal(2))), true)
    checkEvaluation(In(Literal(3), Seq(Literal(1), Literal(2))), false)
    checkEvaluation(
      And(In(Literal(1), Seq(Literal(1), Literal(2))), In(Literal(2), Seq(Literal(1), Literal(2)))),
      true)

    checkEvaluation(In(Literal("^Ba*n"), Seq(Literal("^Ba*n"))), true)
    checkEvaluation(In(Literal("^Ba*n"), Seq(Literal("aa"), Literal("^Ba*n"))), true)
    checkEvaluation(In(Literal("^Ba*n"), Seq(Literal("aa"), Literal("^n"))), false)
  }

  test("INSET") {
    val hS = HashSet[Any]() + 1 + 2
    val nS = HashSet[Any]() + 1 + 2 + null
    val one = Literal(1)
    val two = Literal(2)
    val three = Literal(3)
    val nl = Literal(null)
    checkEvaluation(InSet(one, hS), true)
    checkEvaluation(InSet(two, hS), true)
    checkEvaluation(InSet(two, nS), true)
    checkEvaluation(InSet(nl, nS), true)
    checkEvaluation(InSet(three, hS), false)
    checkEvaluation(InSet(three, nS), false)
    checkEvaluation(And(InSet(one, hS), InSet(two, hS)), true)
  }

  private val smallValues = Seq(1, Decimal(1), Array(1.toByte), "a", 0f, 0d).map(Literal(_))
  private val largeValues =
    Seq(2, Decimal(2), Array(2.toByte), "b", Float.NaN, Double.NaN).map(Literal(_))

  private val equalValues1 =
    Seq(1, Decimal(1), Array(1.toByte), "a", Float.NaN, Double.NaN).map(Literal(_))
  private val equalValues2 =
    Seq(1, Decimal(1), Array(1.toByte), "a", Float.NaN, Double.NaN).map(Literal(_))

  test("BinaryComparison: <") {
    for (i <- 0 until smallValues.length) {
      checkEvaluation(smallValues(i) < largeValues(i), true)
      checkEvaluation(equalValues1(i) < equalValues2(i), false)
      checkEvaluation(largeValues(i) < smallValues(i), false)
    }
  }

  test("BinaryComparison: <=") {
    for (i <- 0 until smallValues.length) {
      checkEvaluation(smallValues(i) <= largeValues(i), true)
      checkEvaluation(equalValues1(i) <= equalValues2(i), true)
      checkEvaluation(largeValues(i) <= smallValues(i), false)
    }
  }

  test("BinaryComparison: >") {
    for (i <- 0 until smallValues.length) {
      checkEvaluation(smallValues(i) > largeValues(i), false)
      checkEvaluation(equalValues1(i) > equalValues2(i), false)
      checkEvaluation(largeValues(i) > smallValues(i), true)
    }
  }

  test("BinaryComparison: >=") {
    for (i <- 0 until smallValues.length) {
      checkEvaluation(smallValues(i) >= largeValues(i), false)
      checkEvaluation(equalValues1(i) >= equalValues2(i), true)
      checkEvaluation(largeValues(i) >= smallValues(i), true)
    }
  }

  test("BinaryComparison: ===") {
    for (i <- 0 until smallValues.length) {
      checkEvaluation(smallValues(i) === largeValues(i), false)
      checkEvaluation(equalValues1(i) === equalValues2(i), true)
      checkEvaluation(largeValues(i) === smallValues(i), false)
    }
  }

  test("BinaryComparison: <=>") {
    for (i <- 0 until smallValues.length) {
      checkEvaluation(smallValues(i) <=> largeValues(i), false)
      checkEvaluation(equalValues1(i) <=> equalValues2(i), true)
      checkEvaluation(largeValues(i) <=> smallValues(i), false)
    }
  }

  test("BinaryComparison: null test") {
    val normalInt = Literal(1)
    val nullInt = Literal.create(null, IntegerType)

    def nullTest(op: (Expression, Expression) => Expression): Unit = {
      checkEvaluation(op(normalInt, nullInt), null)
      checkEvaluation(op(nullInt, normalInt), null)
      checkEvaluation(op(nullInt, nullInt), null)
    }

    nullTest(LessThan)
    nullTest(LessThanOrEqual)
    nullTest(GreaterThan)
    nullTest(GreaterThanOrEqual)
    nullTest(EqualTo)

    checkEvaluation(normalInt <=> nullInt, false)
    checkEvaluation(nullInt <=> normalInt, false)
    checkEvaluation(nullInt <=> nullInt, true)
  }
}
