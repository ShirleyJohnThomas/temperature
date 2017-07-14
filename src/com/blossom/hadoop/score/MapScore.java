package com.blossom.hadoop.score;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @description:
 * @author Blossom
 * @time 2017 下午1:53:14
 */
public class MapScore extends Mapper<LongWritable, Text, Text, IntWritable>{

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		//按照换行符分割
		StringTokenizer tokenizer = new StringTokenizer(line,"\n");
		//分别对每一行进行处理
		StringTokenizer stringTokenizer = null;
		String name = null;
		String score = null;
		Text studentName = null;
		int studentScore = 0;
		while (tokenizer.hasMoreElements()) {
			//每行按照空格分割
			stringTokenizer = new StringTokenizer(tokenizer.nextToken());
			name = stringTokenizer.nextToken(); //学生姓名
			score = stringTokenizer.nextToken(); //学生成绩
			
			studentName = new Text(name);
			studentScore = Integer.valueOf(score);
			
			context.write(studentName, new IntWritable(studentScore));
		}
		
	}

	
	
}
