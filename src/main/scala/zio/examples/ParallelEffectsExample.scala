package zio.examples

import zio.{Task, URIO, ZIO}

object ParallelEffectsExample extends zio.App {

  def run(args: List[String]): ZIO[zio.ZEnv, Nothing, Int] =
    myAppLogic.fold(e => throw e, _ => 0)

  val myEffect1: URIO[Any, Int] =
    ZIO.effect {
      //throw new RuntimeException("Exception 1")
      (1 to 10).foreach { _ =>
        println("Effect 1 works!")
      }
    }.fold(e => {
      println(e); 1
    }, _ => 0)

  val myEffect2: URIO[Any, Int] =
    ZIO.effect {
      //throw new RuntimeException("Exception 2")
      (1 to 10).foreach { _ =>
        println("Effect 2 works!")
      }
    }.fold(e => {
      println(e); 1
    }, _ => 0)

  val myAppLogic: ZIO[Any, Throwable, Unit] =
    for {
      fiber1 <- myEffect1.fork
      fiber2 <- myEffect2.fork
      fiber = fiber1.zip(fiber2)
      numOfFailures <- fiber.join
      _ <- if (numOfFailures._1 + numOfFailures._2 >= 2) { Task.fail(new RuntimeException("Job failed!")) }
           else Task.succeed("Success!")
    } yield ()
}
