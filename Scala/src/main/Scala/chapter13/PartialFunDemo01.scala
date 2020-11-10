package chapter13

/*
给你一个集合val list = List(1, 2, 3, 4, "abc") ，请完成如下要求:
将集合list中的所有数字+1，并返回一个新的集合
要求忽略掉 非数字 的元素，即返回的 新的集合 形式为 (2, 3, 4, 5)

 */
object PartialFunDemo01 {
  def main(args: Array[String]): Unit = {
    //思路1 filter + map 方式解决
    //虽然可以解决问题，但是麻烦.

    val list = List(1, 2, 3, 4, "hello")
    // 先过滤，再map
    println(list.filter(f1).map(f3).map(f2))

    //思路2-模式匹配
    //小结：虽然使用模式匹配比较简单，但是不够完美
    val list2 = list.map(addOne2)
    println("list2=" + list2)


  }

  //模式匹配
  def addOne2(i: Any): Any = {
    i match {
      case x: Int => x + 1
      case _ =>
    }
  }


  def f1(n: Any): Boolean = {
    n.isInstanceOf[Int]
  }

  def f2(n: Int): Int = {
    n + 1
  }

  //将Any->Int [map]
  def f3(n: Any): Int = {
    n.asInstanceOf[Int]
  }
}
