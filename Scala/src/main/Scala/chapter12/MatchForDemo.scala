package chapter12

object MatchForDemo {
  def main(args: Array[String]): Unit = {
    val map = Map("A" -> 1, "B" -> 0, "C" -> 3)
    for ((k, v) <- map) {
      println(k + " -> " + v) // 出来三个key-value ("A"->1), ("B"->0), ("C"->3)
    }
    //说明 : 只遍历出 value =0 的key-value ,其它的过滤掉
    println("--------------(k, 0) <- map-------------------")
    for ((k, 0) <- map) {
      println(k + " --> " + 0)
    }

    //说明, 这个就是上面代码的另外写法, 只是下面的用法灵活和强大
    println("--------------(k, v) <- map if v == 0-------------------")
    for ((k, v) <- map if v >= 1) {
      println(k + " ---> " + v)
    }

  }
}


