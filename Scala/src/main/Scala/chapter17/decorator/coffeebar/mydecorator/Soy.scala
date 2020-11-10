package chapter17.decorator.coffeebar.mydecorator


class Soy(obj: Drink) extends Decorator(obj) {
  setDescription("Soy")
  setPrice(1.5f)
}
