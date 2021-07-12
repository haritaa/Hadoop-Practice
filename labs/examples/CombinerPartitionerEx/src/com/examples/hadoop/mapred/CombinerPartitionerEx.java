package com.examples.hadoop.mapred;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CombinerPartitionerEx {

	/**
	 * @method main This method is used for setting all the configuration
	 *         properties. It acts as a driver for map reduce code.
	 */
	public static void main(String[] args) throws Exception {
		// reads the default configuration of cluster
		Configuration conf = new Configuration();

		// Initializing the job with the default configuration of the cluster
		Job job = Job.getInstance(conf, "wordcount");
		// Assigning the driver class name
		job.setJarByClass(CombinerPartitionerEx.class);

		// Defining the mapper class name
		job.setMapperClass(Map.class);
		// *******************************
		// Defining the combiner class name
		job.setCombinerClass(Reduce.class);
		// *******************************
		// Defining the partitioner class name
		job.setPartitionerClass(MyPartitioner.class);
		job.setNumReduceTasks(3);
		// *******************************
		// Defining the reducer class name
		job.setReducerClass(Reduce.class);
		// Key type coming out of mapper
		job.setOutputKeyClass(Text.class);
		// value type coming out of mapper
		job.setOutputValueClass(IntWritable.class);

		// Use TextInputFormat, the default
		// unless job.setInputFormatClass is used
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// exiting the job only if the flag value becomes false
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	/**
	 * Mapper class. Splits input record into tokens and maps no of occurences
	 * for each word.
	 */
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private boolean caseSensitive = false;

		protected void setup(Mapper.Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			this.caseSensitive = config.getBoolean("wordcount.case.sensitive", false);
		}

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
			String lineStr = lineText.toString();
			// Case insensitive match check
			if (!caseSensitive) {
				lineStr = lineStr.toLowerCase();
			}
			StringTokenizer itr = new StringTokenizer(lineStr);
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	/**
	 * Partitioner class. Partitions mapper output and sends to reducer. Output
	 * types of Mapper should be same as arguments of Partitioner
	 */
	public static class MyPartitioner extends Partitioner<Text, IntWritable> {

		@Override
		public int getPartition(Text key, IntWritable value, int numPartitions) {

			String myKey = key.toString().toLowerCase();

			if (myKey.equals("hadoop")) {
				return 0;
			}
			if (myKey.equals("data")) {
				return 1;
			} else {
				return 2;
			}
		}
	}

	/**
	 * Reducer class. Aggregates no of occurrences for each word.
	 */
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text word, Iterable<IntWritable> counts, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable count : counts) {
				sum += count.get();
			}
			context.write(word, new IntWritable(sum));
		}
	}
}
