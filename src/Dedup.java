


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.blossom.hadoop.dedup.Map;
import com.blossom.hadoop.dedup.Reduce;

/**
 * @description:
 * @author Blossom
 * @time 2017 上午11:54:18
 */
public class Dedup {
	
 
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        //这句话很关键
        conf.set("mapred.job.tracker", "hdfs://106.14.2.96:9001/");
       
     @SuppressWarnings("deprecation")
	Job job = new Job(conf, "Data Deduplication");
     
     job.setJarByClass(Dedup.class);
     
     //设置Map、Combine和Reduce处理类
     job.setMapperClass(Map.class);
     job.setCombinerClass(Reduce.class);
     job.setReducerClass(Reduce.class);
     
     //设置输出类型
     job.setOutputKeyClass(Text.class);
     job.setOutputValueClass(Text.class);
     
     //设置输入和输出目录
     FileInputFormat.addInputPath(job, new Path("hdfs://106.14.2.96:9000/dedup_input"));
     FileOutputFormat.setOutputPath(job, new Path("hdfs://106.14.2.96:9000/dedup_output"));
     System.exit(job.waitForCompletion(true) ? 0 : 1);
     }
}
