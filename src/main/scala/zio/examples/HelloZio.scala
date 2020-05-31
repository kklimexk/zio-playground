package zio.examples

import zio.console._

object HelloZio extends zio.App {

  def run(args: List[String]) =
    myAppLogic.exitCode

  val myAppLogic =
    for {
      _ <- putStrLn("Hello! What is your name?")
      name <- getStrLn
      _ <- putStrLn(s"Hello, $name, welcome to ZIO!")
    } yield ()
}
