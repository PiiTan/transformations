package thoughtworks.citibike

import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object CitibikeTransformerUtils {
  private final val MetersPerFoot = 0.3048
  private final val FeetPerMile = 5280

  final val EarthRadiusInM: Double = 6371e3
  final val MetersPerMile: Double = MetersPerFoot * FeetPerMile

  implicit class StringDataset(val dataSet: Dataset[Row]) {

    def computeDistances(spark: SparkSession) = {
      def calculateDistanceUDF = udf((lat1: Float, lon1:Float, lat2:Float, lon2:Float) => {
        val dLat = scala.math.toRadians(lat2-lat1);
        val dLon = scala.math.toRadians(lon2-lon1);
        val a = Math.sin(dLat/2) * Math.sin(dLat/2) +
                  Math.cos(scala.math.toRadians(lat1)) * Math.cos(scala.math.toRadians(lat2)) *
                  Math.sin(dLon/2) * Math.sin(dLon/2)

        val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
        val distance =  Math.floor(EarthRadiusInM * c / MetersPerMile * 100)/100
        distance
      })
      spark.udf.register("calculateDistance", calculateDistanceUDF)
      val dataSetWithDistance = dataSet.withColumn("distance",
        calculateDistanceUDF(col("start_station_latitude"), col("start_station_longitude"),
          col("end_station_latitude"), col("end_station_longitude")))
      dataSetWithDistance
    }
  }
}
