
import org.apache.spark._
import org.apache.spark.SparkContext._

object SimpleSQL {
    def main(arges: Array[String]) {
      val conf = new SparkConf().setAppName("simpleSQL")

      // Create a Scala Spark Context.
      val sc = new SparkContext(conf)

      val sqlContext = new org.apache.spark.sql.SQLContext(sc)

      import sqlContext.implicits._


      val df = sqlContext.read.json("examples/src/main/resources/people.json")

      df.show()
    }
}
