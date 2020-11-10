package scala_base

/**
 * 基本上java中语法是一样的
 */
object ifesedemo {

  def main(args: Array[String]): Unit = {
    /**
     * 这是的左右都是闭区间
     */
    for (i <- 1 to 3) {
      print(i + "--")
    }
    println()
    println("-----------------------")

    /**
     * 这是左闭右开的区间
     */
    for (i <- 1 until 3) {
      print(i + "--")
    }
    println()
    println("-----------------------")

    /**
     * 循环守卫，即循环保护式（也称条件判断式，守卫）。 保护式为 true 则进入循环体内部，为 false 则跳过，类似于 continue
     */
    for (i <- 1 to 3 if i != 2) {
      print(i + "*** ")
    }

    /**
     * 双层for循环的写法是什么
     */
    for (i <- 1 to 3; j <- 1 to 3) {
      println(s"i=$i j=$j")
    }
    //等价的写法
    println("================")
    for (i <- 1 to 3) {
      for (j <- 1 to 3) {
        println(s"i=$i j=$j")
      }
    }
  }

  def test02() = {
    var res: Unit = if (5 > 2) {
      println("5")
    } else {
      println("2")
    }
  }

  def testswitch() = {
    //可以同时使用的变量的定义就是相当于是的java中多变量的定义
    val (month, age, ticket) = (8, 20, 60.0)
    var buyPrice = 0.0
    if (month >= 4 && month <= 10) {
      if (age >= 18 && age <= 60) {
        buyPrice = ticket
      } else if (age < 18) {
        buyPrice = ticket / 2
      } else {
        buyPrice = ticket / 3
      }
    } else {
      if (age >= 18 && age <= 60) {
        buyPrice = 40.0
      } else {
        buyPrice = 20.0
      }
    }
    println("应付" + buyPrice)
  }

  /**
   * scala中的for的书写方式改变了
   */
  def testfor() = {
    val n = 10
    for (i <- 1 to n) {
      println("你好庄小焱" + i) //i 在不断的变化
    }
    //这里 1 to n 也可以直接是一个集合对象
    val list = List("北京", "广州", "深圳")
    for (item <- list) {
      println("item=" + item)
    }
  }
}

