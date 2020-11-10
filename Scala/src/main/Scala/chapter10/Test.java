package chapter10;

interface AA {

}

public class Test {
    public static void main(String[] args) {
        BB bb = new BB();
        AA aa = bb;
        CC cc = new CC();
        aa = cc;
    }
}

class BB implements AA {

}

class CC implements AA {

}
