package chapter04.myfor

object ForVar {
  def main(args: Array[String]): Unit = {
    for (i <- 1 to 3; j = 4 - i) {
      print(j + " ") // 3,2,1
    }

  }
}
