package com.ldl.demo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * fileName:ReduceTest
 * description:
 * author:Ldl
 * createTime:2019-04-03 11:35
 *
 * 这星是MR程序reducer阶段处理的类
 *
 * KEYIN：就是reducer阶段输入的数帮key类型，对应mapper的输出key类型在本案例中就是单词Text
 *
 * VALUEIN就是reducer阶段输入的数据value类型，对napper的输出value类型在本案例中就是单词次数
 *
 * KEYOUT就是reducer阶段新出的数据key类型在本案例中就是单词 Text
 *
 * VALUEOUTreducer阶段新出的数据value类型在本案例中就是单词的总次数 Intwritable
 *
 */

public class ReduceTest extends Reducer<Text, IntWritable, Text, IntWritable> {
    /**
     * 这里是reduce阶段业务的实现方法
     * 本方法 就收所有来自mapper阶段处理的数据
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     *
     * reduce 接做所有来自map阶段处理的数据之后，我照key的字典序进行排序
     * <hello,1><hadoop,1><spark,I><hadoop,I>
     * 排序后：
     * <hadoop,1><hadoop,I><hello,I><spark,I>
     *
     * 按照key是否相同作为一组去调用reduce方法
     * 本方法的key就是这一组相同kv对的共同key
     * 把这一组所有的y作为一个送代器传入我们的reduce方法
     *
     * <hadoop, [1,1]>
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //定义一个计数器
        int count = 0;
        //遍历迭代器
        for (IntWritable value : values) {
            count += value.get();//返回当前值
        }
        context.write(key,new IntWritable(count));

    }
}
