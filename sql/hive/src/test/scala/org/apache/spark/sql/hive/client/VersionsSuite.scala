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

package org.apache.spark.sql.hive.client

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.sql.catalyst.util.quietly
import org.apache.spark.util.Utils

/**
 * A simple set of tests that call the methods of a hive ClientInterface, loading different version
 * of hive from maven central.  These tests are simple in that they are mostly just testing to make
 * sure that reflective calls are not throwing NoSuchMethod error, but the actually functionality
 * is not fully tested.
 */
class VersionsSuite extends SparkFunSuite with Logging {
  private def buildConf() = {
    lazy val warehousePath = Utils.createTempDir()
    lazy val metastorePath = Utils.createTempDir()
    metastorePath.delete()
    Map(
      "javax.jdo.option.ConnectionURL" -> s"jdbc:derby:;databaseName=$metastorePath;create=true",
      "hive.metastore.warehouse.dir" -> warehousePath.toString)
  }

  test("SPARK-8020: successfully create a HiveContext with metastore settings in Spark conf.") {
    val sparkConf =
      new SparkConf() {
        // We are not really clone it. We need to keep the custom getAll.
        override def clone: SparkConf = this

        override def getAll: Array[(String, String)] = {
          val allSettings = super.getAll
          val metastoreVersion = get("spark.sql.hive.metastore.version")
          val metastoreJars = get("spark.sql.hive.metastore.jars")

          val others = allSettings.filterNot { case (key, _) =>
            key == "spark.sql.hive.metastore.version" || key == "spark.sql.hive.metastore.jars"
          }

          // Put metastore.version to the first one. It is needed to trigger the exception
          // caused by SPARK-8020. Other problems triggered by SPARK-8020
          // (e.g. using Hive 0.13.1's metastore client to connect to the a 0.12 metastore)
          // are not easy to test.
          Array(
            ("spark.sql.hive.metastore.version" -> metastoreVersion),
            ("spark.sql.hive.metastore.jars" -> metastoreJars)) ++ others
        }
      }
    sparkConf
      .set("spark.sql.hive.metastore.version", "12")
      .set("spark.sql.hive.metastore.jars", "maven")

    val hiveContext = new HiveContext(
      new SparkContext(
        "local[2]",
        "TestSQLContextInVersionsSuite",
        sparkConf)) {

      protected override def configure(): Map[String, String] = buildConf

    }

    // Make sure all metastore related lazy vals got created.
    hiveContext.tables()
  }

  test("success sanity check") {
    val badClient = IsolatedClientLoader.forVersion("13", buildConf()).client
    val db = new HiveDatabase("default", "")
    badClient.createDatabase(db)
  }

  private def getNestedMessages(e: Throwable): String = {
    var causes = ""
    var lastException = e
    while (lastException != null) {
      causes += lastException.toString + "\n"
      lastException = lastException.getCause
    }
    causes
  }

  private val emptyDir = Utils.createTempDir().getCanonicalPath

  private def partSpec = {
    val hashMap = new java.util.LinkedHashMap[String, String]
    hashMap.put("key", "1")
    hashMap
  }

  // Its actually pretty easy to mess things up and have all of your tests "pass" by accidentally
  // connecting to an auto-populated, in-process metastore.  Let's make sure we are getting the
  // versions right by forcing a known compatibility failure.
  // TODO: currently only works on mysql where we manually create the schema...
  ignore("failure sanity check") {
    val e = intercept[Throwable] {
      val badClient = quietly { IsolatedClientLoader.forVersion("13", buildConf()).client }
    }
    assert(getNestedMessages(e) contains "Unknown column 'A0.OWNER_NAME' in 'field list'")
  }

  private val versions = Seq("12", "13")

  private var client: ClientInterface = null

  versions.foreach { version =>
    test(s"$version: create client") {
      client = null
      client = IsolatedClientLoader.forVersion(version, buildConf()).client
    }

    test(s"$version: createDatabase") {
      val db = HiveDatabase("default", "")
      client.createDatabase(db)
    }

    test(s"$version: createTable") {
      val table =
        HiveTable(
          specifiedDatabase = Option("default"),
          name = "src",
          schema = Seq(HiveColumn("key", "int", "")),
          partitionColumns = Seq.empty,
          properties = Map.empty,
          serdeProperties = Map.empty,
          tableType = ManagedTable,
          location = None,
          inputFormat =
            Some(classOf[org.apache.hadoop.mapred.TextInputFormat].getName),
          outputFormat =
            Some(classOf[org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat[_, _]].getName),
          serde =
            Some(classOf[org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe].getName()))

      client.createTable(table)
    }

    test(s"$version: getTable") {
      client.getTable("default", "src")
    }

    test(s"$version: listTables") {
      assert(client.listTables("default") === Seq("src"))
    }

    test(s"$version: currentDatabase") {
      assert(client.currentDatabase === "default")
    }

    test(s"$version: getDatabase") {
      client.getDatabase("default")
    }

    test(s"$version: alterTable") {
      client.alterTable(client.getTable("default", "src"))
    }

    test(s"$version: set command") {
      client.runSqlHive("SET spark.sql.test.key=1")
    }

    test(s"$version: create partitioned table DDL") {
      client.runSqlHive("CREATE TABLE src_part (value INT) PARTITIONED BY (key INT)")
      client.runSqlHive("ALTER TABLE src_part ADD PARTITION (key = '1')")
    }

    test(s"$version: getPartitions") {
      client.getAllPartitions(client.getTable("default", "src_part"))
    }

    test(s"$version: loadPartition") {
      client.loadPartition(
        emptyDir,
        "default.src_part",
        partSpec,
        false,
        false,
        false,
        false)
    }

    test(s"$version: loadTable") {
      client.loadTable(
        emptyDir,
        "src",
        false,
        false)
    }

    test(s"$version: loadDynamicPartitions") {
      client.loadDynamicPartitions(
        emptyDir,
        "default.src_part",
        partSpec,
        false,
        1,
        false,
        false)
    }
  }
}
