package spark.streaming.examples

import spark.streaming.StreamingContext
import spark.streaming.StreamingContext._
import spark.streaming.Seconds
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration


object FileStream {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: FileStream <master> <new HDFS compatible directory>")
      System.exit(1)
    }
    
    // Create the context
    val ssc = new StreamingContext(args(0), "FileStream", Seconds(1))

    // Create the new directory 
    val directory = new Path(args(1))
    val fs = directory.getFileSystem(new Configuration())
    if (fs.exists(directory)) throw new Exception("This directory already exists")
    fs.mkdirs(directory)
    fs.deleteOnExit(directory)
    
    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    val inputStream = ssc.textFileStream(directory.toString)
    val words = inputStream.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    
    // Creating new files in the directory
    val text = "This is a text file"
    for (i <- 1 to 30) {
      ssc.sc.parallelize((1 to (i * 10)).map(_ => text), 10)
            .saveAsTextFile(new Path(directory, i.toString).toString)
      Thread.sleep(1000)
    }
    Thread.sleep(5000) // Waiting for the file to be processed 
    ssc.stop()
    System.exit(0)
  }
}