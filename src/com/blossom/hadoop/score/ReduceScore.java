package com.blossom.hadoop.score;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * @description:
 * @author Blossom
 * @time 2017 下午2:00:03
 */
public class ReduceScore extends Reducer<Text, IntWritable, Text, IntWritable> {

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		int sum = 0;
		int count = 0;
		Iterator<IntWritable> iterator = values.iterator();
		while (iterator.hasNext()) {
			sum += iterator.next().get();
			count ++;
		}
		int average = (int) sum/count;
		
		context.write(key, new IntWritable(average));
	}

}
