package org.conan.myhadoop.mr;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class WordCount {

	public static class WordCountMapper extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				output.collect(word, one);
			}

		}
	}

	public static class WordCountReducer extends MapReduceBase
			implements Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
				Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			result.set(sum);
			output.collect(key, result);
		}

	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(WordCount.class);
		String hfdfhead = "hdfs://master:9000";
		String input = hfdfhead + "/wc/in";
		String output = hfdfhead + "/wc/out";
		Path in = new Path(input);
		Path out = new Path(output);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(out)) {
			fs.delete(out, true);
		}

		conf.setJobName("myWordCount");
		// conf.addResource("classpath:/hadoop/core-site.xml");
		// conf.addResource("classpath:/hadoop/hdfs-site.xml");
		// conf.addResource("classpath:/hadoop/mapred-site.xml");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(WordCountMapper.class);
		conf.setCombinerClass(WordCountReducer.class);
		conf.setReducerClass(WordCountReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, in);
		FileOutputFormat.setOutputPath(conf, out);

		JobClient.runJob(conf);
		System.exit(0);
	}

}