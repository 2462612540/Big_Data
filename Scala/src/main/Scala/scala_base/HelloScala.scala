package scala_base

/**
 * 1. object表示一一个伴生对象，这里我们可以简单的理解就是一个对象
 * 2. HelloScala就是对象名字，它底层真正对应的类名是HelloScala$,对象是HelloScala$ 类型的- -个静态对象MODULE$
 * 3.当我们编写一个object HelloScala底层会生成两个.class文件分别是HelloScala和HelloScala$
 * 4. scala在运行时，的流程如下
 * (1)先从HelloScala 的main开始执行
 * public static void main(StringD paramArrayOfString)
 * {
 * HelloScala$.MODULE$.main(paramArrayOfString);
 * }
 * (2)然后调用HelloScala$类的方法HelloScala$ .MODULE$.main
 * (3) 即执行了下面的代码
 * //public void main(String[ args)
 * {
 *    PredefMODUL E$ printn"hello,scala!~~");
 * }
 * object HelloScala {
 * //1. def表示是- -个方法，这是一个关键字
 * //2. main表示方法名字，表示程序入口
 * //3. args: Array[String]表示形参，scala 的特点是讲参数名在前，类型后
 * //4. Array[String]表示类型数组
 * //5.:Unit=表示该函数的返回值为空(void)
 * //6. printn("hello,scala!~")输出一句话
 */
object HelloScala {
  def main(args: Array[String]): Unit = {
    printf("Hello word scala ")
  }

  def test(): Unit = {
    printf("这里测试的函数")
  }
}
