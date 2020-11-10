package chapter17.abstractfactory.pizzastore.order

trait AbsFactory {

  //一个抽象方法
  def createPizza(t: String): Pizza

}
