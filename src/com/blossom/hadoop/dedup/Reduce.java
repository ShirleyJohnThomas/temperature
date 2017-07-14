package com.blossom.hadoop.dedup;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @description:
 * @author Blossom
 * @time 2017 上午11:37:26
 */
public class Reduce extends Reducer<Text,Text,Text,Text> {
	//实现reduce函数
    public void reduce(Text key,Iterable<Text> values,Context context)
            throws IOException,InterruptedException{
        context.write(key, new Text(""));
    }
}
