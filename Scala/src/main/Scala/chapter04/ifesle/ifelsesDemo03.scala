package chapter04.ifesle

import scala.io.StdIn

object ifelsesDemo03 {
  def main(args: Array[String]): Unit = {
    /**
     * 岳小鹏参加scala考试，他和父亲岳不群达成承诺：
     * 如果：
     * 成绩为100分时，奖励一辆BMW；
     * 成绩为(80，99]时，奖励一台iphone7plus；
     * 当成绩为[60,80]时，奖励一个 iPad；
     * 其它时，什么奖励也没有。
     * 成绩是从控制台输入
     */
    println("请输入成绩")
    val score = StdIn.readDouble()
    if (score == 100) {
      println("成绩为100分时，奖励一辆BMW")
    } else if (score > 80 && score <= 99) { //写法1使用范围，写法2就是严格的判断
      println("成绩为(80，99]时，奖励一台iphone7plus")
    } else if (score >= 60 && score <= 80) {
      println("奖励一个 iPad")
    } else {
      println("没有任何奖励")
    }
  }
}
