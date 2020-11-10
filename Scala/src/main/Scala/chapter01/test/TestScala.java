package chapter01.test;

//我们可以理解scala 在运行时，做了一个包装.
public class TestScala {
    public static void main(String[] paramArrayOfString) {
        //TestScala..MODULE$.main(paramArrayOfString);
        TestScala$.MODULE$.main(paramArrayOfString);
    }
}

final class TestScala$ {
    public static final TestScala$ MODULE$;

    static {
        MODULE$ = new TestScala$();
    }

    public void main(String[] args) {
        System.out.println("hello,scala, idea...");
    }
    //private TestScala$() { MODULE$ = this; }
}