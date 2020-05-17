package zio.examples

import java.io.IOException

import zio.ZIO
import zio.console._

object HelloZio extends zio.App {

  def run(args: List[String]): ZIO[zio.ZEnv, Nothing, Int] =
    myAppLogic.fold(_ => 1, _ => 0)

  val myAppLogic: ZIO[Console, IOException, Unit] =
    for {
      _    <- putStrLn("Hello! What is your name?")
      name <- getStrLn
      _    <- putStrLn(s"Hello, ${name}, welcome to ZIO!")
    } yield ()
}
