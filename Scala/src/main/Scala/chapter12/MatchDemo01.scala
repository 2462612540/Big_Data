package chapter12

object MatchDemo01 {
  def main(args: Array[String]): Unit = {
    val oper = '+'
    val n1 = 20
    val n2 = 10
    var res = 0
    //说明
    //1. match (类似java switch) 和  case 是关键字
    //2. 如果匹配成功， 则 执行 => 后面的代码块.
    //3. 匹配的顺序是从上到下，匹配到一个就执行对应的 代码
    //4. => 后面的代码块 不要写 break ,会自动的退出match
    //5. 如果一个都没有匹配到，则执行 case _ 后面的代码块
    oper match {
      case '+' => {
        res = n1 + n2
        println("ok~~")
        println("hello~~")
      }
      case '-' => res = n1 - n2
      case '*' => res = n1 * n2
      case '/' => res = n1 / n2
      case 1 => println("匹配到1")
      case 1.1 => println("匹配1.1")
      case _ => println("oper error")
    }
    println("res=" + res)

  }
}
