package com.ldl.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * fileName:JobTest
 * description:
 * author:Ldl
 * createTime:2019-04-03 11:57
 *
 * 这个类就是nr程序运行时候的主类，本类中组装了一些程序运行时候所需要的信息
 *
 * 比如：使用的是那个Mapper类那个Reducer类输入数帮在那输出数据在什么她方
 */

public class JobTest {
    public static void main(String[] args) throws Exception {
        //通过job 封装本次工作mr相关信息
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        //指定运行主类
        job.setJarByClass(JobTest.class);
        //指定本次mr所用的mapper 和 reduce
        job.setMapperClass(MapTest.class);
        job.setReducerClass(ReduceTest.class);
        //指定mpaaer输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //指定最终输出结果类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //指定输入数据和输出数据存放的路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //提交
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
