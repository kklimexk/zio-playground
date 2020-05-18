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
      println(e);
      1
    }, _ => 0)

  val myEffect2: URIO[Any, Int] =
    ZIO.effect {
      //throw new RuntimeException("Exception 2")
      (1 to 10).foreach { _ =>
        println("Effect 2 works!")
      }
    }.fold(e => {
      println(e);
      1
    }, _ => 0)

  val myEffect3: URIO[Any, Int] =
    ZIO.effect {
      //throw new RuntimeException("Exception 3")
      (1 to 10).foreach { _ =>
        println("Effect 3 works!")
      }
    }.fold(e => {
      println(e);
      1
    }, _ => 0)

  val effectsToRun = Seq(myEffect1, myEffect2, myEffect3)

  val myAppLogic: ZIO[Any, Throwable, Unit] =
    for {
      r <- ZIO.reduceAllPar(effectsToRun.head, effectsToRun.tail)(_ + _)
      _ <- if (r >= effectsToRun.size) Task.fail(new RuntimeException("Job failed!"))
           else if (r == 0) Task.succeed("Job fully success!")
           else Task.succeed("Job partially success!")
    } yield ()
}
