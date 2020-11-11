package chapter04.ifesle

object Exercise01 {
  def main(args: Array[String]): Unit = {
    /*
    【选作】定义两个变量Int，判断二者的和，是否既能被3又能被5整除，打印提示信息
     */
    val num1 = 10
    val num2 = 5
    val sum = num1 + num2
    if (sum % 3 == 0 && sum % 5 == 0) {
      println("能被3又能被5整除")
    } else {
      println("能被3又能被5整除 不成立~")
    }
    /*
    判断一个年份是否是闰年，闰年的条件是符合下面二者之一：(1)年份能被4整除，但不能被100整除；(2)能被400整除
     */
    //定义一个变量保存年份
    val year = 2018
    if ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)) {
      println(s"${year} 是闰年...")
    } else {
      println(s"${year} 不是闰年")
    }
  }
}
