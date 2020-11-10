package chapter17.decorator.coffeebar.mydecorator


class NewMilk(obj: Drink) extends Decorator(obj) {

  setDescription("新式Milk")
  setPrice(4.0f)
}