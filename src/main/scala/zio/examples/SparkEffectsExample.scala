package zio.examples

import org.apache.spark.sql.SparkSession
import zio.examples.common._
import zio._

object SparkEffectsExample {

  final case class Event(id: Int, name: String)
  final case class Config(targetTablePath: String, partitionKey: String)

  val configurationLayer: Layer[Nothing, Has[Config]] =
    ZLayer.succeed(Config("tmp/events", "id"))

  def main(args: Array[String]): Unit = {
    val layers = sparkSessionLayer ++ configurationLayer

    zio.Runtime.default.unsafeRun(myAppLogic.provideLayer(layers))
  }

  def myEffect1(c: Config)(implicit spark: SparkSession): URIO[Any, Int] =
    ZIO.effect {
      import spark.implicits._
      //throw new RuntimeException("Exception 1")
      val events = Seq(Event(1, "Event1"), Event(2, "Event2"), Event(3, "Event3")).toDF()
      events.write.format("delta").partitionBy(c.partitionKey).mode("append").save(c.targetTablePath)
    }.fold(e => {
      println(e);
      1
    }, _ => 0)

  def myEffect2(c: Config)(implicit spark: SparkSession): URIO[Any, Int] =
    ZIO.effect {
      import spark.implicits._
      //throw new RuntimeException("Exception 2")
      val events = Seq(Event(4, "Event4"), Event(5, "Event5"), Event(6, "Event6")).toDF()
      events.write.format("delta").partitionBy(c.partitionKey).mode("append").save(c.targetTablePath)
    }.fold(e => {
      println(e);
      1
    }, _ => 0)

  def myEffect3(c: Config)(implicit spark: SparkSession): URIO[Any, Int] =
    ZIO.effect {
      import spark.implicits._
      //throw new RuntimeException("Exception 3")
      val events = Seq(Event(7, "Event7"), Event(8, "Event8"), Event(9, "Event9")).toDF()
      events.write.format("delta").partitionBy(c.partitionKey).mode("append").save(c.targetTablePath)
    }.fold(e => {
      println(e);
      1
    }, _ => 0)

  def checkDeltaTableEffect(c: Config)(implicit spark: SparkSession): Task[Unit] =
    ZIO.effect {
      val res = spark.read.format("delta").load(c.targetTablePath)
      res.groupBy(c.partitionKey).count().sort("count").show(false)
    }

  val myAppLogic: ZIO[Has[SparkSession] with Has[Config], Throwable, Unit] =
    for {
      implicit0(spark: SparkSession) <- ZIO.access[Has[SparkSession]](_.get[SparkSession])
      config <- ZIO.access[Has[Config]](_.get[Config])
      effectsToRun = Seq(myEffect1(config), myEffect2(config), myEffect3(config))
      r <- ZIO.reduceAll(effectsToRun.head, effectsToRun.tail)(_ + _)
      _ <- if (r >= effectsToRun.size) Task.fail(new RuntimeException("Job failed!"))
      else if (r == 0) Task.succeed("Job fully success!")
      else Task.succeed("Job partially success!")
      _ <- checkDeltaTableEffect(config)
    } yield ()
}
