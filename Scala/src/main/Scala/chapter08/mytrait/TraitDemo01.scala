package chapter08.mytrait

object TraitDemo01 {
  def main(args: Array[String]): Unit = {

  }
}

//trait Serializable extends Any with java.io.Serializable
//在scala中，java的接口都可以当做trait来使用(如上面的语法)
object T1 extends Serializable {
}

object T2 extends Cloneable {

}

