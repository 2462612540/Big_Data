package com.atguigu.chapter18.covariantcontravariant

object Demo {
  def main(args: Array[String]): Unit = {
    val t1: Temp3[Sub] = new Temp3[Sub]("hello");
    //ok
    //      val t2: Temp3[Sub] = new Temp3[Super]("hello");//error
    //      val t3: Temp3[Super] = new Temp3[Sub]("hello");//error
    val t4: Temp3[Sub] = new Temp3[Sub]("hello"); //ok
    val t5: Temp4[Super] = new Temp4[Sub]("hello"); //ok
    //val t6: Temp4[Sub] = new Temp4[Super]("hello"); //ok
    val t7: Temp5[Sub] = new Temp5[Sub]("hello"); //ok
    val t8: Temp5[Sub] = new Temp5[Super]("hello"); //ok
    //val t9: Temp5[Super] = new Temp5[Sub]("hello"); //ok


  }
}


//协变
class Temp4[+A](title: String) { //Temp3[+A] //Temp[-A]
  override def toString: String = {
    title
  }
}

//逆变
class Temp5[-A](title: String) { //Temp3[+A] //Temp[-A]
  override def toString: String = {
    title
  }
}

//不变
class Temp3[A](title: String) { //Temp3[+A] //Temp[-A]
  override def toString: String = {
    title
  }
}

//支持协变
class Super //父类

//Sub是Super的子类
class Sub extends Super

