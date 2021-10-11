import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class MapReduce {

	public static void main(String[] args) {
		JobClient my_client = new JobClient();
		
		JobConf job_conf = new JobConf(MapReduce.class);
		
		job_conf.setJobName("Orders");

		job_conf.setOutputKeyClass(Text.class);
		job_conf.setOutputValueClass(IntWritable.class);

		job_conf.setMapperClass(MapFor.class);
		job_conf.setReducerClass(ReduceFor.class);

		job_conf.setInputFormat(TextInputFormat.class);
		job_conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job_conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(job_conf, new Path(args[1]));
		
		my_client.setConf(job_conf);
		try {
			// Run the job 
			JobClient.runJob(job_conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static class MapFor extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final IntWritable one = new IntWritable(1);

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

			String valueString = value.toString();
			String[] SingleData = valueString.split(",");
			output.collect(new Text(SingleData[8]), one);
		}
	}
	
	public static class ReduceFor extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text t_key, Iterator<IntWritable> values, OutputCollector<Text,IntWritable> output, Reporter reporter) throws IOException {
			Text key = t_key;
			int frequency = 0;
			while (values.hasNext()) {
				IntWritable value = (IntWritable) values.next();
				frequency += value.get();
			}
			output.collect(key, new IntWritable(frequency));
		}
	}


}
