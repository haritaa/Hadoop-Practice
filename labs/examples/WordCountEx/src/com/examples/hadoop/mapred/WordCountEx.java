package com.examples.hadoop.mapred;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountEx {

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
		job.setJarByClass(WordCountEx.class);

		// Defining the mapper class name
		job.setMapperClass(Map.class);
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
		//Jack Bill Joe
		//Don Don Joe
		//Jack Don Bill
		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(lineText.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
			//Jack 1
			//Bill 1
			//Joe 1
			
			//Don 1
			//Don 1
			//Joe 1
			
			//Jack 1
			//Don 1
			//Bill 1
		}
	}

	/**
	 * Reducer class. Aggregates no of occurrences for each word.
	 */
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		//Bill (1,1)
		//Don  (1,1,1)
		//Jack (1,1)
		//Joe  (1,1)
		@Override
		public void reduce(Text word, Iterable<IntWritable> counts, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable count : counts) {
				sum += count.get();
			}
			context.write(word, new IntWritable(sum));
		}
		//Bill 2
		//Don 3
		//Jack 2
		//Joe 2
	}
}
