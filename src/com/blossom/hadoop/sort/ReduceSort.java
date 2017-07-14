package com.blossom.hadoop.sort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @description:
 * @author Blossom
 * @time 2017 下午1:13:58
 */
public class ReduceSort extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{

	
	 private static IntWritable linenum = new IntWritable(1);
	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@SuppressWarnings("unused")
	@Override
	protected void reduce(IntWritable key, Iterable<IntWritable> values,
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)
					throws IOException, InterruptedException {
		for(IntWritable data:values){
			context.write(linenum, key);
			linenum = new IntWritable(linenum.get()+1);
		}
	}

}
