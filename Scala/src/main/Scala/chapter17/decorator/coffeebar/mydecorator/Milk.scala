package chapter17.decorator.coffeebar.mydecorator


class Milk(obj: Drink) extends Decorator(obj) {

  setDescription("Milk")
  setPrice(2.0f)
}
