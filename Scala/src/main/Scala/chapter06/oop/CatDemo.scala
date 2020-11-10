package chapter06.oop

object CatDemo {
  def main(args: Array[String]): Unit = {

    //创建一只猫
    val cat = new Cat
    //给猫的属性赋值

    //说明
    //1. cat.name = "小白" 其实不是直接访问属性，而是  cat.name_$eq("小白")
    //2. cat.name 等价于 cat.name()
    cat.name = "小白" //等价
    cat.age = 10
    cat.color = "白色"
    println("ok~")
    printf("\n小猫的信息如下: %s %d %s", cat.name, cat.age, cat.color)
  }
}

//定义一个类Cat
//一个class Cat 对应的字节码文件只有一个 Cat.class ,默认是public
class Cat {
  //定义/声明三个属性
  //说明
  //1. 当我们声明了 var name :String时, 在底层对应 private name
  //2. 同时会生成 两个public方法 name() <=类似=> getter  public name_$eq() => setter
  var name: String = "" //给初始值
  var age: Int = _ // _ 表示给age 一个默认的值 ，如果Int 默认就是0
  var color: String = _ // _ 给 color 默认值，如果String ,默认是就是""
}

/*
public class Cat
{
  private String name = "";
  private int age;
  private String color;

  public String name()
  {
    return this.name; }
  public void name_$eq(String x$1) { this.name = x$1; }
  public int age() { return this.age; }
  public void age_$eq(int x$1) { this.age = x$1; }
  public String color() { return this.color; }
  public void color_$eq(String x$1) { this.color = x$1; }

}
 */

class Dog {
  var name = "jack"
  var lover = new Fish
}

class Fish {

}