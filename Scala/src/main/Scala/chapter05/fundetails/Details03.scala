package chapter05.fundetails

object Details03 {
  def main(args: Array[String]): Unit = {

    def f1(): Unit = { //ok private final
      println("f1")
    }

    println("ok~~")

    def sayOk(): Unit = { // private final sayOk$1 ()
      println("main sayOk")

      def sayOk(): Unit = { //  private final sayOk$2 ()
        println("sayok sayok")
      }
    }

  }

  def sayOk(): Unit = { //成员
    println("main sayOk")
  }
}
