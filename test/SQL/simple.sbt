name := "SQL Project"

version := "1.0"

scalaVersion := "2.10.4"

//libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.1"

libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "1.5.1",
        "org.apache.spark" %% "spark-sql"  % "1.5.1"
    )
