package chapter12

object MatchExercise02 {
  def main(args: Array[String]): Unit = {

    for (ch <- "+-3!") {
      var sign = 0
      var digit = 0
      ch match {
        case _ if ch > 10000 => digit = 3
        case '+' => sign = 1
        case '-' => sign = -1
        // 说明..
        case _ => println("没有任何匹配~~~")
      }
      println(ch + " " + sign + " " + digit)
    }

  }
}
