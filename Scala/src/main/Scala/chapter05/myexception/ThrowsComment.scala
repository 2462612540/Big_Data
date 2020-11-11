package chapter05.myexception

object ThrowsComment {
  def main(args: Array[String]): Unit = {
    f11()
  }

  @throws(classOf[NumberFormatException]) //等同于Java NumberFormatException.class
  def f11() = {
    "abc".toInt
  }
}
