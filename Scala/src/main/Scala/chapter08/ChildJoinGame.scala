package chapter08

object ChildJoinGame {
  def main(args: Array[String]): Unit = {
    //创建三个小孩
    val child0 = new Child02("白骨精")
    val child1 = new Child02("蜘蛛精")
    val child2 = new Child02("黄鼠狼精")
    Child02.joinGame(child0)
    Child02.joinGame(child1)
    Child02.joinGame(child2)
    Child02.showNum()
  }
}

class Child02(cName: String) {
  var name = cName
}

object Child02 {
  //统计共有多少小孩的属性
  var totalChildNum = 0

  def joinGame(child: Child02): Unit = {
    printf("%s 小孩加入了游戏\n", child.name)
    //totalChildNum 加1
    totalChildNum += 1
  }

  def showNum(): Unit = {
    printf("当前有%d小孩玩游戏\n", totalChildNum)
  }
}