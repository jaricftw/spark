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

package org.apache.spark.ml.recommendation

import java.util.Random

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.scalatest.FunSuite

import org.apache.spark.Logging
import org.apache.spark.ml.recommendation.ALS._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}

class ALSSuite extends FunSuite with MLlibTestSparkContext with Logging {

  private var sqlContext: SQLContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sqlContext = new SQLContext(sc)
  }

  test("LocalIndexEncoder") {
    val random = new Random
    for (numBlocks <- Seq(1, 2, 5, 10, 20, 50, 100)) {
      val encoder = new LocalIndexEncoder(numBlocks)
      val maxLocalIndex = Int.MaxValue / numBlocks
      val tests = Seq.fill(5)((random.nextInt(numBlocks), random.nextInt(maxLocalIndex))) ++
        Seq((0, 0), (numBlocks - 1, maxLocalIndex))
      tests.foreach { case (blockId, localIndex) =>
        val err = s"Failed with numBlocks=$numBlocks, blockId=$blockId, and localIndex=$localIndex."
        val encoded = encoder.encode(blockId, localIndex)
        assert(encoder.blockId(encoded) === blockId, err)
        assert(encoder.localIndex(encoded) === localIndex, err)
      }
    }
  }

  test("normal equation construction with explict feedback") {
    val k = 2
    val ne0 = new NormalEquation(k)
      .add(Array(1.0f, 2.0f), 3.0f)
      .add(Array(4.0f, 5.0f), 6.0f)
    assert(ne0.k === k)
    assert(ne0.triK === k * (k + 1) / 2)
    assert(ne0.n === 2)
    // NumPy code that computes the expected values:
    // A = np.matrix("1 2; 4 5")
    // b = np.matrix("3; 6")
    // ata = A.transpose() * A
    // atb = A.transpose() * b
    assert(Vectors.dense(ne0.ata) ~== Vectors.dense(17.0, 22.0, 29.0) relTol 1e-8)
    assert(Vectors.dense(ne0.atb) ~== Vectors.dense(27.0, 36.0) relTol 1e-8)

    val ne1 = new NormalEquation(2)
      .add(Array(7.0f, 8.0f), 9.0f)
    ne0.merge(ne1)
    assert(ne0.n === 3)
    // NumPy code that computes the expected values:
    // A = np.matrix("1 2; 4 5; 7 8")
    // b = np.matrix("3; 6; 9")
    // ata = A.transpose() * A
    // atb = A.transpose() * b
    assert(Vectors.dense(ne0.ata) ~== Vectors.dense(66.0, 78.0, 93.0) relTol 1e-8)
    assert(Vectors.dense(ne0.atb) ~== Vectors.dense(90.0, 108.0) relTol 1e-8)

    intercept[IllegalArgumentException] {
      ne0.add(Array(1.0f), 2.0f)
    }
    intercept[IllegalArgumentException] {
      ne0.add(Array(1.0f, 2.0f, 3.0f), 4.0f)
    }
    intercept[IllegalArgumentException] {
      val ne2 = new NormalEquation(3)
      ne0.merge(ne2)
    }

    ne0.reset()
    assert(ne0.n === 0)
    assert(ne0.ata.forall(_ == 0.0))
    assert(ne0.atb.forall(_ == 0.0))
  }

  test("normal equation construction with implicit feedback") {
    val k = 2
    val alpha = 0.5
    val ne0 = new NormalEquation(k)
      .addImplicit(Array(-5.0f, -4.0f), -3.0f, alpha)
      .addImplicit(Array(-2.0f, -1.0f), 0.0f, alpha)
      .addImplicit(Array(1.0f, 2.0f), 3.0f, alpha)
    assert(ne0.k === k)
    assert(ne0.triK === k * (k + 1) / 2)
    assert(ne0.n === 0) // addImplicit doesn't increase the count.
    // NumPy code that computes the expected values:
    // alpha = 0.5
    // A = np.matrix("-5 -4; -2 -1; 1 2")
    // b = np.matrix("-3; 0; 3")
    // b1 = b > 0
    // c = 1.0 + alpha * np.abs(b)
    // C = np.diag(c.A1)
    // I = np.eye(3)
    // ata = A.transpose() * (C - I) * A
    // atb = A.transpose() * C * b1
    assert(Vectors.dense(ne0.ata) ~== Vectors.dense(39.0, 33.0, 30.0) relTol 1e-8)
    assert(Vectors.dense(ne0.atb) ~== Vectors.dense(2.5, 5.0) relTol 1e-8)
  }

  test("CholeskySolver") {
    val k = 2
    val ne0 = new NormalEquation(k)
      .add(Array(1.0f, 2.0f), 4.0f)
      .add(Array(1.0f, 3.0f), 9.0f)
      .add(Array(1.0f, 4.0f), 16.0f)
    val ne1 = new NormalEquation(k)
      .merge(ne0)

    val chol = new CholeskySolver
    val x0 = chol.solve(ne0, 0.0).map(_.toDouble)
    // NumPy code that computes the expected solution:
    // A = np.matrix("1 2; 1 3; 1 4")
    // b = b = np.matrix("3; 6")
    // x0 = np.linalg.lstsq(A, b)[0]
    assert(Vectors.dense(x0) ~== Vectors.dense(-8.333333, 6.0) relTol 1e-6)

    assert(ne0.n === 0)
    assert(ne0.ata.forall(_ == 0.0))
    assert(ne0.atb.forall(_ == 0.0))

    val x1 = chol.solve(ne1, 0.5).map(_.toDouble)
    // NumPy code that computes the expected solution, where lambda is scaled by n:
    // x0 = np.linalg.solve(A.transpose() * A + 0.5 * 3 * np.eye(2), A.transpose() * b)
    assert(Vectors.dense(x1) ~== Vectors.dense(-0.1155556, 3.28) relTol 1e-6)
  }

  test("RatingBlockBuilder") {
    val emptyBuilder = new RatingBlockBuilder()
    assert(emptyBuilder.size === 0)
    val emptyBlock = emptyBuilder.build()
    assert(emptyBlock.srcIds.isEmpty)
    assert(emptyBlock.dstIds.isEmpty)
    assert(emptyBlock.ratings.isEmpty)

    val builder0 = new RatingBlockBuilder()
      .add(Rating(0, 1, 2.0f))
      .add(Rating(3, 4, 5.0f))
    assert(builder0.size === 2)
    val builder1 = new RatingBlockBuilder()
      .add(Rating(6, 7, 8.0f))
      .merge(builder0.build())
    assert(builder1.size === 3)
    val block = builder1.build()
    val ratings = Seq.tabulate(block.size) { i =>
      (block.srcIds(i), block.dstIds(i), block.ratings(i))
    }.toSet
    assert(ratings === Set((0, 1, 2.0f), (3, 4, 5.0f), (6, 7, 8.0f)))
  }

  test("UncompressedInBlock") {
    val encoder = new LocalIndexEncoder(10)
    val uncompressed = new UncompressedInBlockBuilder(encoder)
      .add(0, Array(1, 0, 2), Array(0, 1, 4), Array(1.0f, 2.0f, 3.0f))
      .add(1, Array(3, 0), Array(2, 5), Array(4.0f, 5.0f))
      .build()
    assert(uncompressed.size === 5)
    val records = Seq.tabulate(uncompressed.size) { i =>
      val dstEncodedIndex = uncompressed.dstEncodedIndices(i)
      val dstBlockId = encoder.blockId(dstEncodedIndex)
      val dstLocalIndex = encoder.localIndex(dstEncodedIndex)
      (uncompressed.srcIds(i), dstBlockId, dstLocalIndex, uncompressed.ratings(i))
    }.toSet
    val expected =
      Set((1, 0, 0, 1.0f), (0, 0, 1, 2.0f), (2, 0, 4, 3.0f), (3, 1, 2, 4.0f), (0, 1, 5, 5.0f))
    assert(records === expected)

    val compressed = uncompressed.compress()
    assert(compressed.size === 5)
    assert(compressed.srcIds.toSeq === Seq(0, 1, 2, 3))
    assert(compressed.dstPtrs.toSeq === Seq(0, 2, 3, 4, 5))
    var decompressed = ArrayBuffer.empty[(Int, Int, Int, Float)]
    var i = 0
    while (i < compressed.srcIds.size) {
      var j = compressed.dstPtrs(i)
      while (j < compressed.dstPtrs(i + 1)) {
        val dstEncodedIndex = compressed.dstEncodedIndices(j)
        val dstBlockId = encoder.blockId(dstEncodedIndex)
        val dstLocalIndex = encoder.localIndex(dstEncodedIndex)
        decompressed += ((compressed.srcIds(i), dstBlockId, dstLocalIndex, compressed.ratings(j)))
        j += 1
      }
      i += 1
    }
    assert(decompressed.toSet === expected)
  }

  /**
   * Generates an explicit feedback dataset for testing ALS.
   * @param numUsers number of users
   * @param numItems number of items
   * @param rank rank
   * @param noiseStd the standard deviation of additive Gaussian noise on training data
   * @param seed random seed
   * @return (training, test)
   */
  def genExplicitTestData(
      numUsers: Int,
      numItems: Int,
      rank: Int,
      noiseStd: Double = 0.0,
      seed: Long = 11L): (RDD[Rating], RDD[Rating]) = {
    val trainingFraction = 0.6
    val testFraction = 0.3
    val totalFraction = trainingFraction + testFraction
    val random = new Random(seed)
    val userFactors = genFactors(numUsers, rank, random)
    val itemFactors = genFactors(numItems, rank, random)
    val training = ArrayBuffer.empty[Rating]
    val test = ArrayBuffer.empty[Rating]
    for ((userId, userFactor) <- userFactors; (itemId, itemFactor) <- itemFactors) {
      val x = random.nextDouble()
      if (x < totalFraction) {
        val rating = blas.sdot(rank, userFactor, 1, itemFactor, 1)
        if (x < trainingFraction) {
          val noise = noiseStd * random.nextGaussian()
          training += Rating(userId, itemId, rating + noise.toFloat)
        } else {
          test += Rating(userId, itemId, rating)
        }
      }
    }
    logInfo(s"Generated an explicit feedback dataset with ${training.size} ratings for training " +
      s"and ${test.size} for test.")
    (sc.parallelize(training, 2), sc.parallelize(test, 2))
  }

  /**
   * Generates an implicit feedback dataset for testing ALS.
   * @param numUsers number of users
   * @param numItems number of items
   * @param rank rank
   * @param noiseStd the standard deviation of additive Gaussian noise on training data
   * @param seed random seed
   * @return (training, test)
   */
  def genImplicitTestData(
      numUsers: Int,
      numItems: Int,
      rank: Int,
      noiseStd: Double = 0.0,
      seed: Long = 11L): (RDD[Rating], RDD[Rating]) = {
    // The assumption of the implicit feedback model is that unobserved ratings are more likely to
    // be negatives.
    val positiveFraction = 0.8
    val negativeFraction = 1.0 - positiveFraction
    val trainingFraction = 0.6
    val testFraction = 0.3
    val totalFraction = trainingFraction + testFraction
    val random = new Random(seed)
    val userFactors = genFactors(numUsers, rank, random)
    val itemFactors = genFactors(numItems, rank, random)
    val training = ArrayBuffer.empty[Rating]
    val test = ArrayBuffer.empty[Rating]
    for ((userId, userFactor) <- userFactors; (itemId, itemFactor) <- itemFactors) {
      val rating = blas.sdot(rank, userFactor, 1, itemFactor, 1)
      val threshold = if (rating > 0) positiveFraction else negativeFraction
      val observed = random.nextDouble() < threshold
      if (observed) {
        val x = random.nextDouble()
        if (x < totalFraction) {
          if (x < trainingFraction) {
            val noise = noiseStd * random.nextGaussian()
            training += Rating(userId, itemId, rating + noise.toFloat)
          } else {
            test += Rating(userId, itemId, rating)
          }
        }
      }
    }
    logInfo(s"Generated an implicit feedback dataset with ${training.size} ratings for training " +
      s"and ${test.size} for test.")
    (sc.parallelize(training, 2), sc.parallelize(test, 2))
  }

  /**
   * Generates random user/item factors, with i.i.d. values drawn from U(a, b).
   * @param size number of users/items
   * @param rank number of features
   * @param random random number generator
   * @param a min value of the support (default: -1)
   * @param b max value of the support (default: 1)
   * @return a sequence of (ID, factors) pairs
   */
  private def genFactors(
      size: Int,
      rank: Int,
      random: Random,
      a: Float = -1.0f,
      b: Float = 1.0f): Seq[(Int, Array[Float])] = {
    require(size > 0 && size < Int.MaxValue / 3)
    require(b > a)
    val ids = mutable.Set.empty[Int]
    while (ids.size < size) {
      ids += random.nextInt()
    }
    val width = b - a
    ids.toSeq.sorted.map(id => (id, Array.fill(rank)(a + random.nextFloat() * width)))
  }

  /**
   * Test ALS using the given training/test splits and parameters.
   * @param training training dataset
   * @param test test dataset
   * @param rank rank of the matrix factorization
   * @param maxIter max number of iterations
   * @param regParam regularization constant
   * @param implicitPrefs whether to use implicit preference
   * @param numUserBlocks number of user blocks
   * @param numItemBlocks number of item blocks
   * @param targetRMSE target test RMSE
   */
  def testALS(
      training: RDD[Rating],
      test: RDD[Rating],
      rank: Int,
      maxIter: Int,
      regParam: Double,
      implicitPrefs: Boolean = false,
      numUserBlocks: Int = 2,
      numItemBlocks: Int = 3,
      targetRMSE: Double = 0.05): Unit = {
    val sqlContext = this.sqlContext
    import sqlContext.createSchemaRDD
    val als = new ALS()
      .setRank(rank)
      .setRegParam(regParam)
      .setImplicitPrefs(implicitPrefs)
      .setNumUserBlocks(numUserBlocks)
      .setNumItemBlocks(numItemBlocks)
    val alpha = als.getAlpha
    val model = als.fit(training)
    val predictions = model.transform(test)
      .select("rating", "prediction")
      .map { case Row(rating: Float, prediction: Float) =>
        (rating.toDouble, prediction.toDouble)
      }
    val rmse =
      if (implicitPrefs) {
        // TODO: Use a better (rank-based?) evaluation metric for implicit feedback.
        // We limit the ratings and the predictions to interval [0, 1] and compute the weighted RMSE
        // with the confidence scores as weights.
        val (totalWeight, weightedSumSq) = predictions.map { case (rating, prediction) =>
          val confidence = 1.0 + alpha * math.abs(rating)
          val rating01 = math.max(math.min(rating, 1.0), 0.0)
          val prediction01 = math.max(math.min(prediction, 1.0), 0.0)
          val err = prediction01 - rating01
          (confidence, confidence * err * err)
        }.reduce { case ((c0, e0), (c1, e1)) =>
          (c0 + c1, e0 + e1)
        }
        math.sqrt(weightedSumSq / totalWeight)
      } else {
        val mse = predictions.map { case (rating, prediction) =>
          val err = rating - prediction
          err * err
        }.mean()
        math.sqrt(mse)
      }
    logInfo(s"Test RMSE is $rmse.")
    assert(rmse < targetRMSE)
  }

  test("exact rank-1 matrix") {
    val (training, test) = genExplicitTestData(numUsers = 20, numItems = 40, rank = 1)
    testALS(training, test, maxIter = 1, rank = 1, regParam = 1e-5, targetRMSE = 0.001)
    testALS(training, test, maxIter = 1, rank = 2, regParam = 1e-5, targetRMSE = 0.001)
  }

  test("approximate rank-1 matrix") {
    val (training, test) =
      genExplicitTestData(numUsers = 20, numItems = 40, rank = 1, noiseStd = 0.01)
    testALS(training, test, maxIter = 2, rank = 1, regParam = 0.01, targetRMSE = 0.02)
    testALS(training, test, maxIter = 2, rank = 2, regParam = 0.01, targetRMSE = 0.02)
  }

  test("approximate rank-2 matrix") {
    val (training, test) =
      genExplicitTestData(numUsers = 20, numItems = 40, rank = 2, noiseStd = 0.01)
    testALS(training, test, maxIter = 4, rank = 2, regParam = 0.01, targetRMSE = 0.03)
    testALS(training, test, maxIter = 4, rank = 3, regParam = 0.01, targetRMSE = 0.03)
  }

  test("different block settings") {
    val (training, test) =
      genExplicitTestData(numUsers = 20, numItems = 40, rank = 2, noiseStd = 0.01)
    for ((numUserBlocks, numItemBlocks) <- Seq((1, 1), (1, 2), (2, 1), (2, 2))) {
      testALS(training, test, maxIter = 4, rank = 2, regParam = 0.01, targetRMSE = 0.03,
        numUserBlocks = numUserBlocks, numItemBlocks = numItemBlocks)
    }
  }

  test("more blocks than ratings") {
    val (training, test) =
      genExplicitTestData(numUsers = 4, numItems = 4, rank = 1)
    testALS(training, test, maxIter = 2, rank = 1, regParam = 1e-4, targetRMSE = 0.002,
     numItemBlocks = 5, numUserBlocks = 5)
  }

  test("implicit feedback") {
    val (training, test) =
      genImplicitTestData(numUsers = 20, numItems = 40, rank = 2, noiseStd = 0.01)
    testALS(training, test, maxIter = 4, rank = 2, regParam = 0.01, implicitPrefs = true,
      targetRMSE = 0.3)
  }
}
