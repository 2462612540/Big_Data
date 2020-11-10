package com.atguigu.chapter18.lowerbounds


//
//1）和Animal直系的，是Animal父类的还是父类处理，是Animal子类的按照Animal处理()，
//2）和Animal无关的，一律按照Object处理！
object LowerBoundsDemo01 {
  def main(args: Array[String]): Unit = {
    println("ok!")
    //满足下界的约束
    biophony(Seq(new Earth, new Earth)).map(_.sound())
    //满足下界的约束
    biophony(Seq(new Animal, new Animal)).map(_.sound())

    //这里我们不能使用上界的思路去推导，这里是可以运行
    //1.?
    println("===================")
    biophony(Seq(new Bird, new Bird)).map(_.sound()) //

    //biophony(Seq(new Moon))

  }

  //下界
  def biophony[T >: Animal](things: Seq[T]) = things
}


class Earth { //Earth 类
  def sound() { //方法
    println("hello !")
  }
}

class Animal extends Earth {
  override def sound() = { //重写了Earth的方法sound()
    println("animal sound")
  }
}

class Bird extends Animal {
  override def sound() = { //将Animal的方法重写
    print("bird sounds")
  }
}

class Moon {
  //  def sound()={ //将Animal的方法重写
  //    print("bird sounds")
  //  }
}
