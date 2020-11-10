package chapter10;

import java.util.HashSet;

public class JavaHashSet {
    public static void main(String[] args) {
        //java中的Set的元素 没有顺序，不能重复
        HashSet hs = new HashSet<String>();
        hs.add("jack");
        hs.add("tom");
        hs.add("jack");
        hs.add("jack2");
        System.out.println(hs);

    }
}
