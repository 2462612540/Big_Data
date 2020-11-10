package chapter10;

import java.util.ArrayList;

public class JavaCollection {
    public static void main(String[] args) {
        //不可变集合类似java的数组
        int[] nums = new int[3];
        nums[2] = 11; //?
        nums[2] = 22;
        //nums[3] = 90; //?

//        String[] names = {"bj", "sh"};
//        System.out.println(nums + " " + names);
//
//        //可变集合举例
        ArrayList al = new ArrayList<String>();
        al.add("zs");
        al.add("zs2");
        System.out.println(al + " 地址= " + al.hashCode()); //地址
        al.add("zs3");
        System.out.println(al + " 地址2=" + al.hashCode()); //地址

    }
}
