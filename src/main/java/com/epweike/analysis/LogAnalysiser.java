package com.epweike.analysis;

import java.net.URI;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LogAnalysiser extends Configured implements Tool {
	public static void main(String[] args) {
		try {
			int res;
			res = ToolRunner
					.run(new Configuration(), new LogAnalysiser(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args == null || args.length < 2) {
			System.out.println("need inputpath and outputpath");
			return 1;
		}
		
		String shortin = args[0];
		String shortout = args[1];
		
		// 如果输出文件路径存在则删除  
        FileSystem fileSystem = FileSystem.get(new URI(shortout),  
                new Configuration());  
        Path path = new Path(shortout);  
        if (fileSystem.exists(path)) {  
        	System.out.println("outputpath exist,it will be deleted...");
            fileSystem.delete(path, true);  
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Logs Analysis");
		job.setJarByClass(LogAnalysiser.class);
		
		job.setOutputKeyClass(Text.class);// 输出的 key 类型，在 OutputFormat 会检查
		job.setOutputValueClass(NullWritable.class); // 输出的 value 类型，在OutputFormat 会检查
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		
		FileInputFormat.setInputPaths(job, new Path(shortin));// hdfs 中的输入路径
		FileOutputFormat.setOutputPath(job, new Path(shortout));// hdfs 中输出路径

		Date startTime = new Date();
		System.out.println("Job started: " + startTime);
		job.waitForCompletion(true);
		Date end_time = new Date();
		System.out.println("Job ended: " + end_time);
		System.out.println("The job took "
				+ (end_time.getTime() - startTime.getTime()) / 1000
				+ " seconds.");
		return 0;
	}
}