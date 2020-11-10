package chapter04.mydowhile

object Demo01 {
  def main(args: Array[String]): Unit = {
    var i = 0 // for
    do {
      printf(i + "hello,尚硅谷\n")
      i += 1
    } while (i < 10)
  }
}
