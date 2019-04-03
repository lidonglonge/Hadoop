package com.ldl.demo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * fileName:MapTest
 * description:通过MapReduce实现对数据的计算
 * author:Ldl
 * createTime:2019-04-03 10:57
 * 1：继承org.apache.hadoop.mapreduce.Mapper
 * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 * KEXIN：表示napper数据输入的时候key的数据类型，在默认的读取数帮组件下，叫InputFormat，它的行为是一行一行的读取待处理的数据
 *
 * 读取一行，返回一行给我们的mr程序，这种倩况下kevin就表示每一行的起始偏移量因此数帮类型是Long
 *
 * VALUEIN:表napper数据输入的时候value的数帮类型，在默认的读取数据组件下 valuein就表示读取的这一行内容因此数帮类型是string
 *
 * KEXOUT 表示napper数帮输出的时候key的数帮类型在本案例当中输出的key是单词因此数据类型是String
 *
 * VALUEOUT表示napper数辉输出的时候value的数据类型在本案例当中输出的key是单词的次数因此数据类型是 Integer
 *
 * 这星所说的数据类型string Long都是jdk自带的类型在序列化的时候效率低下因此hadoop自己封装一套数据类型
 *
 * long ------> Longwritable
 *
 * String ----> Text
 *
 * Integer ---> Intwritable
 *
 * nul1 ------> Nullwritable
 */

public class MapTest extends Mapper<LongWritable, Text, Text, IntWritable> {
    /**
     * 这星就是Mapper阶段具体的业务逻辑实现方法  该方法的调用取决于读取数据的组件有没有给mr传入数据
     * 如果有的话每传一次，方法执行一次
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //拿到数据将数据转化为String类型
        String lineWord = value.toString();
        //将数据分割
        String[] words = lineWord.split(" ");
        //遍历数据 每出现一个单词 就标记一个数字1 <data,1>
        for (String word : words) {
            //使用mr程序自带的context上下文工具，将mapper阶段数据发送出去
            //作为reduce的输入数据
            context.write(new Text(word),new IntWritable(1));
        }


    }
}
