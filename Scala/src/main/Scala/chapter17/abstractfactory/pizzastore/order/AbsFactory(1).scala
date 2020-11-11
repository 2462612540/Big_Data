package chapter17.abstractfactory.pizzastore.order

import chapter17.abstractfactory.pizzastore.pizza.Pizza


trait AbsFactory {

  //一个抽象方法
  def createPizza(t: String): Pizza

}
