package chapter06.constructor

object ConDemo02 {

  def main(args: Array[String]): Unit = {
    val a = new AA("jack")
    //输出的顺序是
    //1. b~~~ 父类
    //2. AA() 主构造器
    //3. A this(name:String) 辅助构造器
  }
}

class BB() {
  println("b~~~")
}

class AA() extends BB() {
  println("AA()")

  def this(name: String) {
    this
    println("A this(name:String)")
  }
}
