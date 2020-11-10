package chapter13

object HigherOrderFunction {
  def main(args: Array[String]): Unit = {
    def test(f: Double => Double, f2: Double => Int, n1: Double) = {
      f(f2(n1)) // f(0)
    }

    //sum 是接收一个Double,返回一个Double
    def sum(d: Double): Double = {
      d + d
    }

    def mod(d: Double): Int = {
      d.toInt % 2
    }

    val res = test(sum, mod, 5.0) //
    println("res=" + res) // 2.0

  }
}
