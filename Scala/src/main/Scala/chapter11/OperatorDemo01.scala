package chapter11

object OperatorDemo01 {
  def main(args: Array[String]): Unit = {

    val n1 = 1
    val n2 = 2
    val r1 = n1 + n2 // 3
    val r2 = n1.+(n2) // 3 看Int的源码即可说明


    val monster = new Monster
    monster + 10
    monster.+(10)

    println("monster.money=" + monster.money) // 20

    println(monster ++)
    println(monster.++)
    println("monster.money=" + monster.money) // 22

    !monster
    println("monster.money=" + monster.money) // -22

  }
}

class Monster {
  var money: Int = 0

  //对操作符进行重载 (中置操作符)
  def +(n: Int): Unit = {
    this.money += n
  }

  //对操作符进行重载(后置操作符)
  def ++(): Unit = {
    this.money += 1
  }

  //对操作符进行重载(前置操作符，一元运算符)
  def unary_!(): Unit = {
    this.money = -this.money
  }
}
