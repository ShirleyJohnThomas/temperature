package com.blossom.hadoop.temperature;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @description:
 * @author Blossom
 * @time 2017 下午4:25:35
 */
public class TempMapper extends Mapper<LongWritable,Text,Text,IntWritable>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		System.out.println("Before Mapper: " + key + ", " +value);
		String line = value.toString();
		String year = line.substring(0, 4);
		int tmperature = Integer.parseInt(line.substring(8));
		context.write(new Text(year), new IntWritable(tmperature));
		//打印样本
		System.out.println("=====" + "After Mapper: " + new Text(year) + "," + new IntWritable(tmperature));
	}
}
