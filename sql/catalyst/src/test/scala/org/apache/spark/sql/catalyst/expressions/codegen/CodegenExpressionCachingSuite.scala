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

package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, LeafExpression}
import org.apache.spark.sql.types.{BooleanType, DataType}

/**
 * A test suite that makes sure code generation handles expression internally states correctly.
 */
class CodegenExpressionCachingSuite extends SparkFunSuite {

  test("GenerateUnsafeProjection") {
    val expr1 = MutableExpression()
    val instance1 = UnsafeProjection.create(Seq(expr1))
    assert(instance1.apply(null).getBoolean(0) === false)

    val expr2 = MutableExpression()
    expr2.mutableState = true
    val instance2 = UnsafeProjection.create(Seq(expr2))
    assert(instance1.apply(null).getBoolean(0) === false)
    assert(instance2.apply(null).getBoolean(0) === true)
  }

  test("GenerateProjection") {
    val expr1 = MutableExpression()
    val instance1 = GenerateProjection.generate(Seq(expr1))
    assert(instance1.apply(null).getBoolean(0) === false)

    val expr2 = MutableExpression()
    expr2.mutableState = true
    val instance2 = GenerateProjection.generate(Seq(expr2))
    assert(instance1.apply(null).getBoolean(0) === false)
    assert(instance2.apply(null).getBoolean(0) === true)
  }

  test("GenerateMutableProjection") {
    val expr1 = MutableExpression()
    val instance1 = GenerateMutableProjection.generate(Seq(expr1))()
    assert(instance1.apply(null).getBoolean(0) === false)

    val expr2 = MutableExpression()
    expr2.mutableState = true
    val instance2 = GenerateMutableProjection.generate(Seq(expr2))()
    assert(instance1.apply(null).getBoolean(0) === false)
    assert(instance2.apply(null).getBoolean(0) === true)
  }

  test("GeneratePredicate") {
    val expr1 = MutableExpression()
    val instance1 = GeneratePredicate.generate(expr1)
    assert(instance1.apply(null) === false)

    val expr2 = MutableExpression()
    expr2.mutableState = true
    val instance2 = GeneratePredicate.generate(expr2)
    assert(instance1.apply(null) === false)
    assert(instance2.apply(null) === true)
  }

}


/**
 * An expression with mutable state so we can change it freely in our test suite.
 */
case class MutableExpression() extends LeafExpression with CodegenFallback {
  var mutableState: Boolean = false
  override def eval(input: InternalRow): Any = mutableState

  override def nullable: Boolean = false
  override def dataType: DataType = BooleanType
}
