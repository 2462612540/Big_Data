/**
 * Copyright (C), 2018-2020
 * FileName: WordCountMapper
 * Author:   xjl
 * Date:     2020/6/19 13:57
 * Description: 单词统计类的
 */
package com.shanghaiuniversity.mr.WordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 这里是mapperduce的 程序  mapper阶段的业务的逻辑的实现类
 * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 * <p>
 * KEYIN 表示mapper的数据的数据的数据的类型 在默认的情况下 叫inputformat 他的行为是一行一样的读取的待处理的数据 读取一行返回一行 跟我们的mr的程序
 * 这样的keyin 就是表示的每一行的其实偏移量 因此数据类型的是long 的类型
 * <p>
 * VALUEIN 表示mapper的数据的时候输入的时候value数据类型的，在默认的读取数据组件valuein 就是表示的读取的一行的内容 因此数据类型是string
 * <p>
 * KEYOUT 表示mapper数据的输出的数据输出的类型  在本案例中的输入的key 是单词  因此的是的数据的类型的是string
 * <p>
 * VALUEOUT表示的mapper的数据的输出的value的数据类型 在本案例中的key的是单词次数 因此是的数据的的类型的是Integer
 * <p>
 * 这里所说的数据类型string  long 都是的jdk的自带类型 在序列化的时候效率很低下 因此在hadoop自己封装；额一套的数据的烈性
 * <p>
 * long  longwriteable
 * String Text
 * Integer IntWritable
 * null nullwriteable
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
//    /**
//     * 这里是mapper 就是的具体的业务逻辑的实现方法 该方式的是的调用的是取决于读取的数的组件的有没有给mr传入数据
//     * 如果是有话， 每一个传入《k,v》 该方法就会调用的是
//     *
//     * @param key
//     * @param value
//     * @param context
//     * @throws IOException
//     * @throws InterruptedException
//     */
//    @Override
//    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        //拿到传入一行的内容 把数据的类型的转化为string
//        String line = value.toString();
//        //将一行的内容的按照分割符号进行一行的内容进行切割 切割成为一个单词的数组
//        String[] words = line.split(" ");
//
//        //遍历的数组 没出现一个单词就标记一个数字1 <单词 1>
//        for (String word : words) {
//            //使用mr的程序的上下文的context 把mapper阶段的处理的数据的发送出去
//            //作为一个reduce节点的输入数据
//            context.write(new Text(word), new IntWritable(1));
//            // hadoop   hadoop  Spark ---<hadoop 1> <hadoop 1> <Spark 1>
//        }
//    }

    Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
        System.out.println(key.toString());
        // 1 获取一行
        String line = value.toString();
        // 2 切割单词
        String[] words = line.split(" ");
        // 3 循环写出
        for (String word : words) {
            k.set(word);
            context.write(k, v);
        }
    }
}
