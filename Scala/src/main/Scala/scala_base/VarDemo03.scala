package scala_base

/**
 * 变量声明时，需要初始值
 */
object VarDemo03 {
  def main(args: Array[String]): Unit = {
    //创建一个对象
    //1. dog 是 val 修饰的，即 dog 是不可变，即 dog 的应用不可变
    //2. 但是 dog.name 是 var 他的属性可变
    val dog = new Dog
    //dog = null [error]
    dog.name = "大黄狗"
    var dog2 = new Dog
    dog2 = null
    dog2.name = "小黄瓜"
    //变量声明时，需要初始值（显示初始化）。
    var job: String = "大数据工程师"
  }
}

class Dog {
  var name = "tom" //初始化 name 为 tom
}
