package com.blossom.hadoop.dedup;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @description:
 * @author Blossom
 * @time 2017 上午11:36:44
 */
public class Map extends Mapper<Object,Text,Text,Text>{
	 private static Text line=new Text();//每行数据
     
     //实现map函数
     public void map(Object key,Text value,Context context)
             throws IOException,InterruptedException{
         line=value;
         context.write(line, new Text(""));
     }
}
