package spark.streaming.examples

import spark.streaming._
import spark.streaming.StreamingContext._
import spark.storage.StorageLevel

object KafkaWordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: WordCountNetwork <master> <hostname> <port>")
      System.exit(1)
    }

    // Create the context and set the batch size
    val ssc = new StreamingContext(args(0), "WordCountNetwork")
    ssc.setBatchDuration(Seconds(2))

    // Create a NetworkInputDStream on target ip:port and count the
    // words in input stream of \n delimited test (eg. generated by 'nc') 
    ssc.checkpoint("checkpoint", Time(1000 * 5))
    val lines = ssc.kafkaStream[String](args(1), args(2).toInt, "test_group", Map("test" -> 1), 
      Map(KafkaPartitionKey(0, "test", "test_group", 0) -> 2382))
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()

    // Wait for 12 seconds
    Thread.sleep(12000)
    ssc.stop()

    val newSsc = new StreamingContext("checkpoint")
    newSsc.start()

  }
}
