package thoughtworks.wordcount

import WordCountUtils._
import org.apache.spark.sql.Dataset
import thoughtworks.DefaultFeatureSpecWithSpark


class WordCountUtilsTest extends DefaultFeatureSpecWithSpark {
  import spark.implicits._

  feature("Split Words") {

    scenario("test splitting a dataset of words by spaces") {
      val sampleText = Seq("was a simple", "first thing he")
      val df = spark.createDataset(sampleText)
      val actual = df.splitWords(spark).collect()

      actual should contain theSameElementsAs Set("was", "a", "simple", "first", "thing", "he")
    }

    scenario("test splitting a dataset of words by period") {
      val sampleText = Seq("was a simple.", "first thing he")
      val df = spark.createDataset(sampleText)
      val actual = df.splitWords(spark).collect()

      actual should contain theSameElementsAs Set("was", "a", "simple", "first", "thing", "he")
    }

    scenario("test splitting a dataset of words by comma") {
      val sampleText = Seq("was a simple,", "first thing he")
      val df = spark.createDataset(sampleText)
      val actual = df.splitWords(spark).collect()

      actual should contain theSameElementsAs Set("was", "a", "simple", "first", "thing", "he")
    }

    scenario("test splitting a dataset of words by hypen") {
      val sampleText = Seq("was test-hyphen simple", "first thing he")
      val df = spark.createDataset(sampleText)
      val actual = df.splitWords(spark).collect()

      actual should contain theSameElementsAs Set("was", "test", "hyphen", "simple", "first", "thing", "he")
    }

    scenario("test splitting a dataset of words by semi-colon") {
      val sampleText = Seq("was simple: colon", "first thing he")
      val df = spark.createDataset(sampleText)
      val actual = df.splitWords(spark).collect()

      actual should contain theSameElementsAs Set("was", "colon", "simple", "first", "thing", "he")
    }

    scenario("should split word but preserve 's") {
      val sampleText = Seq("was gatsby's", "first thing he")
      val df = spark.createDataset(sampleText)
      val actual = df.splitWords(spark).collect()

      actual should contain theSameElementsAs Set("was", "gatsby's", "first", "thing", "he")
    }
  }

  feature("Count Words") {
    scenario("basic test case") {
      val sampleText = Seq("was", "was", "simple", "first", "thing", "thing")
      val df = spark.createDataset(sampleText)
      val actual = df.countByWord(spark).collect()

      actual should contain theSameElementsAs Set(("was", 2), ("simple", 1), ("first", 1), ("thing", 2))
    }

    scenario("should not aggregate dissimilar words") {
      val sampleText = Seq("were", "werewolf", "first", "thing", "thing")
      val df = spark.createDataset(sampleText)
      val actual = df.countByWord(spark).collect()

      actual should contain theSameElementsAs Set(("were", 1), ("werewolf", 1), ("first", 1), ("thing", 2))
    }

    scenario("test case insensitivity") {
      val sampleText = Seq("Was", "was", "simple", "first", "thing", "thing")
      val df = spark.createDataset(sampleText)
      val actual = df.countByWord(spark).collect()

      actual should contain theSameElementsAs Set(("was", 2), ("simple", 1), ("first", 1), ("thing", 2))
    }
  }

  feature("Sort Words") {
    scenario("test ordering words") {
      val sampleText = Seq("a", "a", "simple", "first", "thing", "thing")
      val df = spark.createDataset(sampleText)
      val actual = df.countByWord(spark).collect()

      actual should contain theSameElementsInOrderAs Array(("a", 2), ("first", 1), ("simple", 1), ("thing", 2))
    }
  }

}
