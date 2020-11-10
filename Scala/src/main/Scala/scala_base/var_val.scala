package scala_base

/**
 * 基本语法的测试的
 * scala为什么要有可变量 和不可以变量的两种的变量?val 和var?
 * 在实际上我们更多的是的获取/创建一个对象后 读取该对象的属性
 * 或者是修改对象的属性值但是我们改变这个对象本身
 * dog =new Dog() dog.age=10,dog=new Dog()
 * 因为val 没有线程安全问题 因此效率高scala的设计这推荐我们使用val
 * 如果是的对象需要改变的 则使用是var
 * val修饰的变量在编译后，等同于加上final
 *
 * 变量声明的时候是一定初始化的值可以是默认的值
 * 当左右两边都是数值型时，则做加法运算当左右两边有一方为字符串，则做拼接运算
 *
 * Scala与Java有着相同的数据类型，在scala中数据类型都是对象，也就是说scala没有java中的原生类型
 * Scala数据类型分为两大类Anyval(值类型)和AnyRef(引用类型)，注意:不管是AnyVal还是AnyRef都是对象
 *
 */
object var_val {
  def main(args: Array[String]): Unit = {
    var num = 10;
    val num1 = 10;
    printf(s"$num" + "-------------");
    printf(s"$num1")
    //printf和println 是不一样的 ln是要有输入结果
    println(num.isInstanceOf[Int])
    val dog = new Dogtest
    dog.age = 10
    dog.name = "小花"
  }
}

class Dogtest {
  //声明了一个age的属性 给一个默认的值_
  var age: Int = _
  //声明一个名字
  var name: String = ""
}
