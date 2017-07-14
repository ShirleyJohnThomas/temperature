package com.blossom.hadoop.sort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * @description:
 * @author Blossom
 * @time 2017 上午11:56:33
 */
public class MapSort extends Mapper<Object, Text, IntWritable, IntWritable> {
	
	private static IntWritable data = new IntWritable();

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, IntWritable, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		data.set(Integer.valueOf(line));
		context.write(data, new IntWritable(1));
	}

}
