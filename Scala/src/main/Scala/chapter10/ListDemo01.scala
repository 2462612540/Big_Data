package chapter10

object ListDemo01 {
  def main(args: Array[String]): Unit = {
    //说明
    //1. 在默认情况下 List 是scala.collection.immutable.List,即不可变
    //2. 在scala中,List就是不可变的，如需要使用可变的List,则使用ListBuffer
    //3. List 在 package object scala 做了 val List = scala.collection.immutable.List
    //4. val Nil = scala.collection.immutable.Nil // List()

    val list01 = List(1, 2, 3, "Hello") //创建时，直接分配元素
    println(list01)

    val list02 = Nil //空集合
    println(list02)

    //访问List的元素
    val value1 = list01(1) // 1是索引，表示取出第2个元素.
    println("value1=" + value1) // 2


    println("-------------list追加元素后的效果-----------------")
    //通过 :+ 和 +: 给list追加元素(本身的集合并没有变化)
    var list1 = List(1, 2, 3, "abc")
    // :+运算符表示在列表的最后增加数据
    val list2 = list1 :+ 4 // (1,2,3,"abc", 4)
    println(list1) //list1没有变化 (1, 2, 3, "abc"),说明list1还是不可变
    println(list2) //新的列表结果是 [1, 2, 3, "abc", 4]

    val list3 = 10 +: list1 // (10,1, 2, 3, "abc")
    println("list3=" + list3)


    //:: 符号的使用

    val list4 = List(1, 2, 3, "abc")
    //说明 val list5 = 4 :: 5 :: 6 :: list4 :: Nil 步骤
    //1. List()
    //2. List(List(1, 2, 3, "abc"))
    //3. List(6,List(1, 2, 3, "abc"))
    //4. List(5,6,List(1, 2, 3, "abc"))
    //5. List(4,5,6,List(1, 2, 3, "abc"))
    val list5 = 4 :: 5 :: 6 :: list4 :: Nil
    println("list5=" + list5)

    //说明 val list6 = 4 :: 5 :: 6 :: list4 ::: Nil 步骤
    //1. List()
    //2. List(1, 2, 3, "abc")
    //3. List(6,1, 2, 3, "abc")
    //4. List(5,6,1, 2, 3, "abc")
    //5. List(4,5,6,1, 2, 3, "abc")
    val list6 = 4 :: 5 :: 6 :: list4 ::: Nil
    println("list6=" + list6)


  }
}
