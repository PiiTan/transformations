package thoughtworks.wordcount

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

object WordCountUtils {
  implicit class StringDataset(val dataSet: Dataset[String]) {
    def splitWords(spark: SparkSession) = {
      import spark.implicits._
      dataSet.flatMap(line => line.split("[^a-zA-Z']").filter(word => word.nonEmpty))
    }

    def countByWord(spark: SparkSession) = {
      import spark.implicits._
      val data = dataSet.map(s => (s, 1))
        .groupByKey(tuple => tuple._1.toLowerCase())
        .count()
        .sort("value")

      data
    }
  }
}
