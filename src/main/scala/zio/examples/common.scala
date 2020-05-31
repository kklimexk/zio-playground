package zio.examples

import org.apache.spark.sql.SparkSession
import zio._

object common {
  val sparkSessionLayer: TaskLayer[Has[SparkSession]] =
    ZLayer.fromEffect(Task.effect(SparkSession.builder()
      .master("local[*]")
      .appName("SparkApp")
      .getOrCreate()))
}
