package com.blossom.hadoop.temperature;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @description:
 * @author Blossom
 * @time 2017 下午4:26:12
 */
public class TempReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		int maxValue = Integer.MIN_VALUE;
		StringBuffer sBuffer = new StringBuffer();
		//去values最大值
		for(IntWritable value:values){
			maxValue = Math.max(maxValue, value.get());
			sBuffer.append(value).append(",");
		}
		//打印样本
		System.out.println("Before Reduce: " + key + "," + sBuffer.toString());
		context.write(key, new IntWritable(maxValue));
		//打印样本
		System.out.println("=====" + "After Reduce: " + key + ", " + maxValue);
	}
}
