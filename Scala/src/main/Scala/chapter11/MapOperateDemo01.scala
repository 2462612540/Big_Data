package chapter11

object MapOperateDemo01 {
  def main(args: Array[String]): Unit = {

    /*
    请将List(3,5,7) 中的所有元素都 * 2 ，
    将其结果放到一个新的集合中返回，即返回一个新的List(6,10,14), 请编写程序实现.

     */

    val list1 = List(3, 5, 7) //集合
    var list2 = List[Int]() //新的集合，准备放入新的内容
    for (item <- list1) { //遍历
      list2 = list2 :+ item * 2 // 对元素*2 ，然后加入list2集合
    }
    println("list2=" + list2) //List(6,10,14)

    //对上面传统的方法来解决问题的小结
    //1. 优点
    //（1） 处理方法比较直接，好理解
    //2. 缺点
    // (1) 不够简洁，高效
    // (2) 没有体现函数式编程特点 集合=》函数 => 新的集合 =》 函数 ..
    // (3) 不利于处理复杂的数据处理业务

  }


}
