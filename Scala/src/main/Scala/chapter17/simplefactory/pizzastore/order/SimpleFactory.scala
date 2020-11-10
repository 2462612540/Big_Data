package chapter17.simplefactory.pizzastore.order

object SimpleFactory {

  //提供了一个创建Pizza的方法,有需要创建Pizza调用该方法即可
  def createPizza(t: String): Pizza = {
    var pizza: Pizza = null
    if (t.equals("greek")) {
      pizza = new GreekPizza
    } else if (t.equals("pepper")) {
      pizza = new PepperPizza
    } else if (t.equals("cheese")) {
      pizza = new CheesePizza
    }
    return pizza
  }

}
