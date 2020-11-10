package scala_base

object VarDemo01 {
  def main(args: Array[String]): Unit = {
    var name: String = "smith"
    var age: Int = 10
    var gender: Char = '男'
    var isPass: Boolean = true
    var sal: Float = 8907.4f
    var num1 = 100 //就使用到了类型推断
    //在 idea 中，直接可以通过工具看到变量的类型
    //类型确定后，就不能修改，说明Scala 是强数据类型语言
    //num1 = "jack"
    //在声明/定义一个变量时，可以使用 var 或者 val 来修饰， var 修饰的变量可改变， val 修饰的变量不可改
    //因为 val 是线程安全的，因此效率高， scala 推荐使用
    //即能够使用 val ,就不要使用 var
    //var 是可以变的。
    var lover = "小红"
    lover = "小黑"
    //val 是不可变的变量
    val girlFriend = "小白"
    //girlFriend = "小黄"
  }
}
