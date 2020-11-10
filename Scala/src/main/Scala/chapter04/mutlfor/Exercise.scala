package chapter04.mutlfor

import scala.io.StdIn

object Exercise {

  /*
  应用实例：

1.统计三个班成绩情况，每个班有5名同学，求出各个班的平均分和所有班级的平均分[学生的成绩从键盘输入]。
  分析思路
  (1) classNum 表示 班级个数 , stuNum 表示学生个数
  (2) classScore 表示各个班级总分 totalScore 表示所有班级总分
  (3) score 表示各个学生成绩
  (4) 使用循环的方式输入成绩
2.统计三个班及格人数，每个班有5名同学。
3.打印出九九乘法表

   */

  def main(args: Array[String]): Unit = {
    val classNum = 3
    val stuNum = 5
    var score = 0.0 //分数
    var classScore = 0.0 //班级的总分
    var totalScore = 0.0 //所有班级总分
    for (i <- 1 to classNum) {
      //先将 classScore 清0
      classScore = 0.0
      for (j <- 1 to stuNum) {
        printf("请输入第%d班级的第%d个学生的成绩\n", i, j)
        score = StdIn.readDouble()
        classScore += score
      }
      //累计 totalScore
      totalScore += classScore
      printf("第%d班级的平均分为%.2f\n", i, classScore / stuNum)
    }
    printf("所有班级的平均分为%.2f", totalScore / (stuNum * classNum))
  }


}
