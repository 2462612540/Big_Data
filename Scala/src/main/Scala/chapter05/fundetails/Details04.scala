package chapter05.fundetails

object Details04 {
  def main(args: Array[String]): Unit = {
    println(sayOk("mary"))
  }

  //name形参的默认值jack
  def sayOk(name: String = "jack"): String = {
    return name + " ok! "
  }

}
