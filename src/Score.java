
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.blossom.hadoop.score.MapScore;
import com.blossom.hadoop.score.ReduceScore;

/**
 * @description:
 * @author Blossom
 * @time 2017 下午2:06:09
 */
public class Score {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		// 这句话很关键

		conf.set("mapred.job.tracker", "hdfs://192.168.1.2:9001/");

	
		Job job = new Job(conf, "Score Average");

		job.setJarByClass(Score.class);

		// 设置Map、Combine和Reduce处理类

		job.setMapperClass(MapScore.class);

		job.setCombinerClass(ReduceScore.class);

		job.setReducerClass(ReduceScore.class);

		// 设置输出类型

		job.setOutputKeyClass(Text.class);

		job.setOutputValueClass(IntWritable.class);

		// 将输入的数据集分割成小数据块splites，提供一个RecordReder的实现

		job.setInputFormatClass(TextInputFormat.class);

		// 提供一个RecordWriter的实现，负责数据输出

		job.setOutputFormatClass(TextOutputFormat.class);

		// 设置输入和输出目录

		FileInputFormat.addInputPath(job, new Path("hdfs://106.14.2.96:9000/score_input"));

		FileOutputFormat.setOutputPath(job, new Path("hdfs://106.14.2.96:9000/score_output"));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
