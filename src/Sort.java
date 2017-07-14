import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.blossom.hadoop.sort.MapSort;
import com.blossom.hadoop.sort.ReduceSort;

/**
 * @description:
 * @author Blossom
 * @time 2017 下午1:18:11
 */
public class Sort {
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		// 这句话很关键

		conf.set("mapred.job.tracker", "hdfs://106.14.2.96:9001/");

		Job job = new Job(conf, "Data Sort");

		job.setJarByClass(Sort.class);

		// 设置Map和Reduce处理类

		job.setMapperClass(MapSort.class);

		job.setReducerClass(ReduceSort.class);

		// 设置输出类型

		job.setOutputKeyClass(IntWritable.class);

		job.setOutputValueClass(IntWritable.class);

		// 设置输入和输出目录

		FileInputFormat.addInputPath(job, new Path("hdfs://106.14.2.96:9000/sort_input"));

		FileOutputFormat.setOutputPath(job, new Path("hdfs://106.14.2.96:9000/sort_output"));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
