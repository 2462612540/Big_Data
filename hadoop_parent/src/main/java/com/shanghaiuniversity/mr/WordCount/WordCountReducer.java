/**
 * Copyright (C), 2018-2020
 * FileName: WordCountReducer
 * Author:   xjl
 * Date:     2020/6/19 14:46
 * Description: Reducder
 */
package com.shanghaiuniversity.mr.WordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 这里是的reduce的处理的额阶阶段的类
 * KEYIN 是reducer阶段的key的类型 对应的mapper 的输出key类型的在本案例中是单词 Text
 * <p>
 * VALUEIN 就是reducer阶段的value类型 对应的是mapper的类型 在本案例中的额单词的次数 Intwriteable
 * <p>
 * KEYOUT  就是reducer阶段的输出的key的类型 在本案例中的就是text
 * <p>
 * VALUEOUT 就是reducer阶段的输出的value的类型 就是单词的总的次数 intwriteable
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
//    /**
//     * 这里是reduce 具体业务类的实现方法
//     * 《hell 1》 《hadoop 1》 《spark 1》《hell 1》《hadoop 1》
//     *  排序
//     * 《hadoop 1》《hadoop 1》 《hell 1》《hell 1》《spark 1》
//     *  按照key是否相同的作为一组调用的reduce方法
//     *  本方法的key及时这一组的相同的kv 的共同的key
//     *   把这一组的所有的v作为一个迭代器传输我们的reduce方法
//     * @param key
//     * @param values
//     * @param context
//     * @throws IOException
//     * @throws InterruptedException reduce 接受所有的mapper的阶段的额处理的数据之后 按照是key的字典来记性排序
//     */
//    @Override
//    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//        //定义一个计数器
//        int count = 0;
//        //遍历一组迭代器 把每一个数量1加起来构成的单词的总数
//        for (IntWritable value : values) {
//            count += value.get();
//        }
//        //把最终的结果输出
//        context.write(key, new IntWritable(count));
//    }

    IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
        int sum = 0;
        // 1 累加求和
        for (IntWritable value : values) {
            sum += value.get();
        }
        v.set(sum);
        context.write(key, v);
    }
}
