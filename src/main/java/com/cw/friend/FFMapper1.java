package com.cw.friend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FFMapper1 extends Mapper<LongWritable, Text, Text, Text> {
    //    private Text k = new Text();
//    private Text v = new Text();
//
//    @Override
//    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        System.out.println(value.toString());
//        String[] split = value.toString().split(":");
//        //关注别人的人作为value
//        v.set(split[0]);
//        String[] men = split[1].split(",");
//        //被关注的人作为key
//        for (String man : men) {
//            k.set(man);
//            System.out.println(k.toString() + "----" + v.toString());
//            context.write(k, v);
//        }
//    }
    private Text k = new Text();
    private Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(":");
        //关注别人的人作为Value
        v.set(split[0]);

        //被关注的人作为Key
        for (String man : split[1].split(",")) {
            k.set(man);
            context.write(k, v);
        }
    }
}
