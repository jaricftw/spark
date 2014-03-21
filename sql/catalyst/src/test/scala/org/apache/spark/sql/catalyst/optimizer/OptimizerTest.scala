package org.apache.spark.sql
package catalyst
package optimizer

import org.scalatest.FunSuite

import types.IntegerType
import util._
import plans.logical.{LogicalPlan, LocalRelation}
import expressions._
import dsl._

/* Implicit conversions for creating query plans */

/**
 * Provides helper methods for comparing plans produced by optimization rules with the expected
 * result
 */
class OptimizerTest extends FunSuite {

  /**
   * Since attribute references are given globally unique ids during analysis,
   * we must normalize them to check if two different queries are identical.
   */
  protected def normalizeExprIds(plan: LogicalPlan) = {
    val minId = plan.flatMap(_.expressions.flatMap(_.references).map(_.exprId.id)).min
    plan transformAllExpressions {
      case a: AttributeReference =>
        AttributeReference(a.name, a.dataType, a.nullable)(exprId = ExprId(a.exprId.id - minId))
    }
  }

  /** Fails the test if the two plans do not match */
  protected def comparePlans(plan1: LogicalPlan, plan2: LogicalPlan) {
    val normalized1 = normalizeExprIds(plan1)
    val normalized2 = normalizeExprIds(plan2)
    if (normalized1 != normalized2)
      fail(
        s"""
          |== FAIL: Plans do not match ===
          |${sideBySide(normalized1.treeString, normalized2.treeString).mkString("\n")}
        """.stripMargin)
  }
}