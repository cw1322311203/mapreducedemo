package com.cw.groupingcomparator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {


    int i = 0;

    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        i++;

        Iterator<NullWritable> iterator = values.iterator();

        while (iterator.hasNext()) {
            NullWritable value = iterator.next();
            context.write(key, value);
        }

    }
}
