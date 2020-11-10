package chapter06.constructor;

public class JavaDemo01 {
    public static void main(String[] args) {

    }
}

class AAA {
    public AAA() {
        //这里调用AAA(String name)
        //super();
        this("aaa");
        System.out.println("AAA()");
    }

    public AAA(String name) {
        super();

    }
}
