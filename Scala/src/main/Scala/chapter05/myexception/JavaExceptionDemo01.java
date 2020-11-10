package chapter05.myexception;

public class JavaExceptionDemo01 {
    public static void main(String[] args) {

        try {
            // 可疑代码
            int i = 0;
            int b = 10;
            int c = b / i; // 执行代码时，会抛出ArithmeticException异常
        } catch (ArithmeticException ex) {
            ex.printStackTrace();
        } catch (Exception e) { //java中不可以把返回大的异常写在前，否则报错!!
            e.printStackTrace();
        } finally {
            // 最终要执行的代码
            System.out.println("java finally");
        }

        System.out.println("ok~~~继续执行...");
    }
}
