package chapter11

import scala.collection.mutable

object Exercise03 {
  def main(args: Array[String]): Unit = {
    val sentence = "AAAAAAAAAABBBBBBBBCCCCCDDDDDDD"
    val map2 = sentence.foldLeft(Map[Char, Int]())(charCount)
    println("map2=" + map2)

    //使用可变的map,效率更高.
    //1. 先创建一个可变map,作为左折叠的第一个参数
    val map3 = mutable.Map[Char, Int]()
    sentence.foldLeft(map3)(charCount2)
    println("map3=" + map3)

  }

  //使用不可变map实现
  def charCount(map: Map[Char, Int], char: Char): Map[Char, Int] = {
    map + (char -> (map.getOrElse(char, 0) + 1))
  }

  //使用可变map实现
  def charCount2(map: mutable.Map[Char, Int], char: Char): mutable.Map[Char, Int] = {
    map += (char -> (map.getOrElse(char, 0) + 1))
  }
}
