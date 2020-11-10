package scala_base

/**
 * 基本语法的测试的
 * scala为什么要有可变量 和不可以变量的两种的变量?val 和var?
 * 在实际上我们更多的是的获取/创建一个对象后 读取该对象的属性
 * 或者是修改对象的属性值但是我们
 *
 */
object var_val_test {
  def main(args: Array[String]): Unit = {
    var num = 10;
    val num1 = 10;

    printf(s"$num" + "-------------");
    printf(s"$num1")
    //printf和println 是不一样的 ln是要有输入结果
    println(num.isInstanceOf[Int])
  }
}
