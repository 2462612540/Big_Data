package chapter12

object MatchExercise01 {
  def main(args: Array[String]): Unit = {
    for (ch <- "+-3!") {
      var sign = 0
      var digit = 0
      ch match {
        case '+' => sign = 1
        case '-' => sign = -1
        // 说明..
        // 可以有多个 默认匹配，但是后面的默认匹配无效，编译器没有报错
        case _ => digit = 3
        case _ => sign = 2
      }
      // + 1 0
      // - -1 0
      // 3 0 3
      // ! 0 3
      println(ch + " " + sign + " " + digit)
    }

  }
}
