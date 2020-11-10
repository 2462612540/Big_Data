package chapter16;

public class Test {
}

class Ticket {

    public void saleTicket() {

        //代码

        //卖票 (互斥)
        synchronized (this) {
            //互斥代码
        }

        //代码
    }
}
