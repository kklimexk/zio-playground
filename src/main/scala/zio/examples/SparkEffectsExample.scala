package zio.examples

import org.apache.spark.sql.SparkSession
import zio.{Task, URIO, ZIO}

object SparkEffectsExample {

  final case class Event(id: Int, name: String)

  val targetTablePath = "tmp/events"
  val partitionKey = "id"

  def main(args: Array[String]): Unit = {
    zio.Runtime.default.unsafeRun(myAppLogic.provide(SparkSession.builder()
      .master("local[*]")
      .appName("SparkParallelEffects")
      .getOrCreate()))
  }

  def myEffect1(implicit spark: SparkSession): URIO[Any, Int] =
    ZIO.effect {
      import spark.implicits._
      //throw new RuntimeException("Exception 1")
      val events = Seq(Event(1, "Event1"), Event(2, "Event2"), Event(3, "Event3")).toDF()
      events.write.format("delta").partitionBy(partitionKey).mode("append").save(targetTablePath)
    }.fold(e => {
      println(e);
      1
    }, _ => 0)

  def myEffect2(implicit spark: SparkSession): URIO[Any, Int] =
    ZIO.effect {
      import spark.implicits._
      //throw new RuntimeException("Exception 2")
      val events = Seq(Event(4, "Event4"), Event(5, "Event5"), Event(6, "Event6")).toDF()
      events.write.format("delta").partitionBy(partitionKey).mode("append").save(targetTablePath)
    }.fold(e => {
      println(e);
      1
    }, _ => 0)

  def myEffect3(implicit spark: SparkSession): URIO[Any, Int] =
    ZIO.effect {
      import spark.implicits._
      //throw new RuntimeException("Exception 3")
      val events = Seq(Event(7, "Event7"), Event(8, "Event8"), Event(9, "Event9")).toDF()
      events.write.format("delta").partitionBy(partitionKey).mode("append").save(targetTablePath)
    }.fold(e => {
      println(e);
      1
    }, _ => 0)

  def checkDeltaTableEffect(implicit spark: SparkSession): Task[Unit] =
    ZIO.effect {
      val res = spark.read.format("delta").load(targetTablePath)
      res.groupBy(partitionKey).count().sort("count").show(false)
    }

  val myAppLogic: ZIO[SparkSession, Throwable, Unit] =
    for {
      implicit0(spark: SparkSession) <- ZIO.environment[SparkSession]
      effectsToRun = Seq(myEffect1, myEffect2, myEffect3)
      r <- ZIO.reduceAll(effectsToRun.head, effectsToRun.tail)(_ + _)
      _ <- if (r >= effectsToRun.size) Task.fail(new RuntimeException("Job failed!"))
      else if (r == 0) Task.succeed("Job fully success!")
      else Task.succeed("Job partially success!")
      _ <- checkDeltaTableEffect
    } yield ()
}
