package zio.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery
import zio.examples.common._
import zio.{Has, Task, ZIO}

object SparkParallelStreamEffectsExample {

  final case class Event(id: Int, name: String)

  val targetTablePath = "tmp/events"

  val targetTablePath1 = "tmp/events1"
  val targetTablePath2 = "tmp/events2"
  val targetTablePath3 = "tmp/events3"

  val partitionKey = "id"

  def main(args: Array[String]): Unit = {
    zio.Runtime.default.unsafeRun(myAppLogic.provideLayer(sparkSessionLayer))
  }

  def myEffect1(implicit spark: SparkSession): Task[StreamingQuery] =
    Task.effect {
      val events = spark.readStream.format("delta").load(targetTablePath)
      events.writeStream
        .format("delta")
        .partitionBy(partitionKey)
        .outputMode("append")
        .option("checkpointLocation", s"$targetTablePath1/_checkpoints")
        .start(targetTablePath1)
    }

  def myEffect2(implicit spark: SparkSession): Task[StreamingQuery] =
    Task.effect {
      val events = spark.readStream.format("delta").load(targetTablePath)
      events.writeStream
        .format("delta")
        .partitionBy(partitionKey)
        .outputMode("append")
        .option("checkpointLocation", s"$targetTablePath2/_checkpoints")
        .start(targetTablePath2)
    }

  def myEffect3(implicit spark: SparkSession): Task[StreamingQuery] =
    Task.effect {
      val events = spark.readStream.format("delta").load(targetTablePath)
      events.writeStream
        .format("delta")
        .partitionBy(partitionKey)
        .outputMode("append")
        .option("checkpointLocation", s"$targetTablePath3/_checkpoints")
        .start(targetTablePath3)
    }

  def awaitAllStreamingQueries(streamingQueries: Seq[StreamingQuery]): Task[Unit] =
    Task.effect(streamingQueries.foreach(_.awaitTermination()))

  def checkDeltaTableEffect(implicit spark: SparkSession): Task[Unit] =
    ZIO.effect {
      val res1 = spark.read.format("delta").load(targetTablePath1)
      res1.groupBy(partitionKey).count().sort("count").show(false)
      val res2 = spark.read.format("delta").load(targetTablePath2)
      res2.groupBy(partitionKey).count().sort("count").show(false)
      val res3 = spark.read.format("delta").load(targetTablePath3)
      res3.groupBy(partitionKey).count().sort("count").show(false)
    }

  val myAppLogic: ZIO[Has[SparkSession], Throwable, Unit] =
    for {
      implicit0(spark: SparkSession) <- ZIO.access[Has[SparkSession]](_.get[SparkSession])
      _ <- SparkEffectsExample.myAppLogic.provideLayer(sparkSessionLayer ++ SparkEffectsExample.configurationLayer)
      effectsToRun = Seq(myEffect1, myEffect2, myEffect3)
      streamingQueries <- ZIO.collectAllPar(effectsToRun)
      _ <- awaitAllStreamingQueries(streamingQueries)
      //_ <- checkDeltaTableEffect
    } yield ()
}
