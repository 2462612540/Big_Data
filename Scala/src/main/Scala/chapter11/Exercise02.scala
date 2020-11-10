package chapter11

import scala.collection.mutable.ArrayBuffer

object Exercise02 {
  def main(args: Array[String]): Unit = {
    val sentence = "AAAAAAAAAABBBBBBBBCCCCCDDDDDDD"
    val arrayBuffer = new ArrayBuffer[Char]()
    //理解折叠的第一个传入的arrayBuffer 含义.
    sentence.foldLeft(arrayBuffer)(putArray)
    println("arrayBuffer=" + arrayBuffer)

  }

  def putArray(arr: ArrayBuffer[Char], c: Char): ArrayBuffer[Char] = {
    //将c 放入到arr 中
    arr.append(c)
    arr
  }
}
