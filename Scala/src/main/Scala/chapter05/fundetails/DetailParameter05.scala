package chapter05.fundetails

object DetailParameter05 {
  def main(args: Array[String]): Unit = {
    //    mysqlCon()
    //    mysqlCon("127.0.0.1", 7777)   //从左到右覆盖

    //如果我们希望指定覆盖某个默认值，则使用带名参数即可,比如修改用户名和密码
    mysqlCon(user = "tom", pwd = "123")

    //f6("v2")  // (错误)
    f6(p2 = "v2") // (?)

  }

  def mysqlCon(add: String = "localhost", port: Int = 3306,
               user: String = "root", pwd: String = "root"): Unit = {
    println("add=" + add)
    println("port=" + port)
    println("user=" + user)
    println("pwd=" + pwd)
  }

  def f6(p1: String = "v1", p2: String) {
    println(p1 + p2);
  }


}
