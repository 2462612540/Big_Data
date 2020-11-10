package chapter08.innerclass;

public class JavaInnerClass {
    public static void main(String[] args) {

        //使用
        //创建一个外部类对象
        OuterClass outer1 = new OuterClass();
        //创建一个外部类对象
        OuterClass outer2 = new OuterClass();

        // 创建Java成员内部类
        // 说明在Java中，将成员内部类当做一个属性，因此使用下面的方式来创建 outer1.new InnerClass().
        OuterClass.InnerClass inner1 = outer1.new InnerClass();
        OuterClass.InnerClass inner2 = outer2.new InnerClass();

        //下面的方法调用说明在java中，内部类只和类型相关，也就是说,只要是
        //OuterClass.InnerClass 类型的对象就可以传给 形参 InnerClass ic
        inner1.test(inner2);
        inner1.test(inner1);

        inner2.test(inner1);

        // 创建Java静态内部类
        // 因为在java中静态内部类是和类相关的，使用 new OuterClass.StaticInnerClass()
        OuterClass.StaticInnerClass staticInner = new OuterClass.StaticInnerClass();

    }
}

class OuterClass { //外部类
    static class StaticInnerClass { //静态内部类
    }

    class InnerClass { //成员内部类
        //test方法可以接收 InnerClass实例
        public void test(InnerClass ic) {
            System.out.println(ic);
        }
    }
}

