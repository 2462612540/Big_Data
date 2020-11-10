package chapter08.homework

/*
编写一个 Scala 应用程序,使用 App 特质,以反序打印命令行参数,用空格隔开。举例来说,scala Reverse Hello World 应该打印 World Hello
 */
object Exercise03 extends App {
  val args2 = args.reverse
  println(args2.mkString(" "))
}
