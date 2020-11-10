package chapter12

object MatchIfDemo01 {
  def main(args: Array[String]): Unit = {
    for (ch <- "+-3!") { //是对"+-3!" 遍历

      var sign = 0
      var digit = 0
      ch match {
        case '+' => sign = 1
        case '-' => sign = -1
        // 说明..
        // 如果 case 后有 条件守卫即if ,那么这时的 _ 不是表示默认匹配
        // 表示忽略 传入 的 ch
        case _ if ch.toString.equals("3") => digit = 3
        case _ if (ch > 1110 || ch < 120) => println("ch > 10")
        case _ => sign = 2
      }
      //分析
      // + 1 0
      // - -1 0
      // 3 0 3
      // ! 2 0
      println(ch + " " + sign + " " + digit)
    }

  }
}
