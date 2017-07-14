
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.blossom.hadoop.temperature.TempMapper;
import com.blossom.hadoop.temperature.TempReducer;

/**
 * @description:
 * @author Blossom
 * @time 2017 上午10:53:34
 */
public class Temperature{
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//输入路径
		String dst = "hdfs://106.14.2.96:9000/input/input.txt";
		//输出路径
		String dstOut = "hdfs://106.14.2.96:9000/output";
		
		Configuration config = new Configuration();
		
		config.set("mapred.job.tracker", "hdfs://106.14.2.96:9001");
		
		config.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		
		config.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		
		@SuppressWarnings("deprecation")
		Job job = new Job(config);
		
		//打成jar包
		job.setJarByClass(Temperature.class);
		
		//job执行作业时输入和输出文件的路径
		FileInputFormat.addInputPath(job, new Path(dst));
		FileOutputFormat.setOutputPath(job, new Path(dstOut));
		
		//指定自定义的Mapper和Reducer作为两个阶段的任务处理类
		job.setMapperClass(TempMapper.class);
		job.setReducerClass(TempReducer.class);
		
		//设置最后输出结果的key和value的类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//执行job直到完成
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		job.waitForCompletion(true);
		System.out.println("finished");
	}
	
}
