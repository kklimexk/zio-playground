package zio.examples

import org.apache.spark.sql.SparkSession
import zio.{Task, URIO, ZIO}

object SparkParallelStreamEffectsExample extends zio.App {

  final case class Event(id: Int, name: String)

  val targetTablePath = "tmp/events"

  val targetTablePath1 = "tmp/events1"
  val targetTablePath2 = "tmp/events2"
  val targetTablePath3 = "tmp/events3"

  val partitionKey = "id"

  def run(args: List[String]): ZIO[zio.ZEnv, Nothing, Int] =
    myAppLogic.provide(SparkSession.builder()
      .master("local[*]")
      .appName("SparkParallelEffects")
      .getOrCreate()).fold(e => throw e, _ => 0)

  def myEffect1(implicit spark: SparkSession): URIO[Any, Int] =
    Task.effectAsync[Nothing] { _ =>
      //throw new RuntimeException("Exception 1")
      val events = spark.readStream.format("delta").load(targetTablePath)
      events.writeStream
        .format("delta")
        .partitionBy(partitionKey)
        .outputMode("append")
        .option("checkpointLocation", s"$targetTablePath1/_checkpoints")
        .start(targetTablePath1)
    }.fold(e => {
      println(e)
      1
    }, _ => 0)

  def myEffect2(implicit spark: SparkSession): URIO[Any, Int] =
    Task.effectAsync[Nothing] { _ =>
      //throw new RuntimeException("Exception 2")
      val events = spark.readStream.format("delta").load(targetTablePath)
      events.writeStream
        .format("delta")
        .partitionBy(partitionKey)
        .outputMode("append")
        .option("checkpointLocation", s"$targetTablePath2/_checkpoints")
        .start(targetTablePath2)
    }.fold(e => {
      println(e)
      1
    }, _ => 0)

  def myEffect3(implicit spark: SparkSession): URIO[Any, Int] =
    Task.effectAsync[Nothing] { _ =>
      //throw new RuntimeException("Exception 3")
      val events = spark.readStream.format("delta").load(targetTablePath)
      events.writeStream
        .format("delta")
        .partitionBy(partitionKey)
        .outputMode("append")
        .option("checkpointLocation", s"$targetTablePath3/_checkpoints")
        .start(targetTablePath3)
    }.fold(e => {
      println(e)
      1
    }, _ => 0)

  def checkDeltaTableEffect(implicit spark: SparkSession): Task[Unit] =
    ZIO.effect {
      val res1 = spark.read.format("delta").load(targetTablePath1)
      res1.groupBy(partitionKey).count().sort("count").show(false)
      val res2 = spark.read.format("delta").load(targetTablePath2)
      res2.groupBy(partitionKey).count().sort("count").show(false)
      val res3 = spark.read.format("delta").load(targetTablePath3)
      res3.groupBy(partitionKey).count().sort("count").show(false)
    }

  val myAppLogic: ZIO[SparkSession, Throwable, Unit] =
    for {
      implicit0(spark: SparkSession) <- ZIO.environment[SparkSession]
      _ <- SparkEffectsExample.myAppLogic.provide(spark)
      effectsToRun = Seq(myEffect1, myEffect2, myEffect3)
      r <- ZIO.reduceAllPar(effectsToRun.head, effectsToRun.tail)(_ + _)
      _ <- if (r >= effectsToRun.size) Task.fail(new RuntimeException("Job failed!"))
           else if (r == 0) Task.succeed("Job fully success!")
           else Task.succeed("Job partially success!")
      //_ <- checkDeltaTableEffect
    } yield ()
}
