package com.cw.topn;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 让所有数据分到同一组的比较器
 */
public class FlowGroupingComparator extends WritableComparator {

    public FlowGroupingComparator() {
        super(FlowBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return 0;
    }
}
