package spark

import java.io.File

import scala.io.Source

import com.google.common.io.Files
import org.scalatest.FunSuite
import org.apache.hadoop.io._

import SparkContext._

class FileSuite extends FunSuite {
  test("text files") {
    val sc = new SparkContext("local", "test")
    val tempDir = Files.createTempDir()
    val outputDir = new File(tempDir, "output").getAbsolutePath
    val nums = sc.makeRDD(1 to 4)
    nums.saveAsTextFile(outputDir)
    // Read the plain text file and check it's OK
    val outputFile = new File(outputDir, "part-00000")
    val content = Source.fromFile(outputFile).mkString
    assert(content === "1\n2\n3\n4\n")
    // Also try reading it in as a text file RDD
    assert(sc.textFile(outputDir).collect().toList === List("1", "2", "3", "4"))
    sc.stop()
  }

  test("SequenceFiles") {
    val sc = new SparkContext("local", "test")
    val tempDir = Files.createTempDir()
    val outputDir = new File(tempDir, "output").getAbsolutePath
    val nums = sc.makeRDD(1 to 3).map(x => (x, "a" * x)) // (1,a), (2,aa), (3,aaa)
    nums.saveAsSequenceFile(outputDir)
    // Try reading the output back as a SequenceFile
    val output = sc.sequenceFile[IntWritable, Text](outputDir)
    assert(output.map(_.toString).collect().toList === List("(1,a)", "(2,aa)", "(3,aaa)"))
    sc.stop()
  }

  test("SequenceFile with writable key") {
    val sc = new SparkContext("local", "test")
    val tempDir = Files.createTempDir()
    val outputDir = new File(tempDir, "output").getAbsolutePath
    val nums = sc.makeRDD(1 to 3).map(x => (new IntWritable(x), "a" * x)) 
    nums.saveAsSequenceFile(outputDir)
    // Try reading the output back as a SequenceFile
    val output = sc.sequenceFile[IntWritable, Text](outputDir)
    assert(output.map(_.toString).collect().toList === List("(1,a)", "(2,aa)", "(3,aaa)"))
    sc.stop()
  }

  test("SequenceFile with writable value") {
    val sc = new SparkContext("local", "test")
    val tempDir = Files.createTempDir()
    val outputDir = new File(tempDir, "output").getAbsolutePath
    val nums = sc.makeRDD(1 to 3).map(x => (x, new Text("a" * x)))
    nums.saveAsSequenceFile(outputDir)
    // Try reading the output back as a SequenceFile
    val output = sc.sequenceFile[IntWritable, Text](outputDir)
    assert(output.map(_.toString).collect().toList === List("(1,a)", "(2,aa)", "(3,aaa)"))
    sc.stop()
  }

  test("SequenceFile with writable key and value") {
    val sc = new SparkContext("local", "test")
    val tempDir = Files.createTempDir()
    val outputDir = new File(tempDir, "output").getAbsolutePath
    val nums = sc.makeRDD(1 to 3).map(x => (new IntWritable(x), new Text("a" * x)))
    nums.saveAsSequenceFile(outputDir)
    // Try reading the output back as a SequenceFile
    val output = sc.sequenceFile[IntWritable, Text](outputDir)
    assert(output.map(_.toString).collect().toList === List("(1,a)", "(2,aa)", "(3,aaa)"))
    sc.stop()
  }

  test("implicit conversions in reading SequenceFiles") {
    val sc = new SparkContext("local", "test")
    val tempDir = Files.createTempDir()
    val outputDir = new File(tempDir, "output").getAbsolutePath
    val nums = sc.makeRDD(1 to 3).map(x => (x, "a" * x)) // (1,a), (2,aa), (3,aaa)
    nums.saveAsSequenceFile(outputDir)
    // Similar to the tests above, we read a SequenceFile, but this time we pass type params
    // that are convertable to Writable instead of calling sequenceFile[IntWritable, Text]
    val output1 = sc.sequenceFile[Int, String](outputDir)
    assert(output1.collect().toList === List((1, "a"), (2, "aa"), (3, "aaa")))
    // Also try having one type be a subclass of Writable and one not
    val output2 = sc.sequenceFile[Int, Text](outputDir)
    assert(output2.map(_.toString).collect().toList === List("(1,a)", "(2,aa)", "(3,aaa)"))
    val output3 = sc.sequenceFile[IntWritable, String](outputDir)
    assert(output3.map(_.toString).collect().toList === List("(1,a)", "(2,aa)", "(3,aaa)"))
    sc.stop()
  }

  test("object files of ints") {
    val sc = new SparkContext("local", "test")
    val tempDir = Files.createTempDir()
    val outputDir = new File(tempDir, "output").getAbsolutePath
    val nums = sc.makeRDD(1 to 4)
    nums.saveAsObjectFile(outputDir)
    // Try reading the output back as an object file
    val output = sc.objectFile[Int](outputDir)
    assert(output.collect().toList === List(1, 2, 3, 4))
    sc.stop()
  }

  test("object files of complex types") {
    val sc = new SparkContext("local", "test")
    val tempDir = Files.createTempDir()
    val outputDir = new File(tempDir, "output").getAbsolutePath
    val nums = sc.makeRDD(1 to 3).map(x => (x, "a" * x))
    nums.saveAsObjectFile(outputDir)
    // Try reading the output back as an object file
    val output = sc.objectFile[(Int, String)](outputDir)
    assert(output.collect().toList === List((1, "a"), (2, "aa"), (3, "aaa")))
    sc.stop()
  }

  test("write SequenceFile using new Hadoop API") {
    import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
    val sc = new SparkContext("local", "test")
    val tempDir = Files.createTempDir()
    val outputDir = new File(tempDir, "output").getAbsolutePath
    val nums = sc.makeRDD(1 to 3).map(x => (new IntWritable(x), new Text("a" * x)))
    nums.saveAsNewAPIHadoopFile[SequenceFileOutputFormat[IntWritable, Text]](
        outputDir)
    val output = sc.sequenceFile[IntWritable, Text](outputDir)
    assert(output.map(_.toString).collect().toList === List("(1,a)", "(2,aa)", "(3,aaa)"))
    sc.stop()
  }

  test("read SequenceFile using new Hadoop API") {
    import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
    val sc = new SparkContext("local", "test")
    val tempDir = Files.createTempDir()
    val outputDir = new File(tempDir, "output").getAbsolutePath
    val nums = sc.makeRDD(1 to 3).map(x => (new IntWritable(x), new Text("a" * x)))
    nums.saveAsSequenceFile(outputDir)
    val output =
        sc.newAPIHadoopFile[IntWritable, Text, SequenceFileInputFormat[IntWritable, Text]](outputDir)
    assert(output.map(_.toString).collect().toList === List("(1,a)", "(2,aa)", "(3,aaa)"))
    sc.stop()
  }
}
