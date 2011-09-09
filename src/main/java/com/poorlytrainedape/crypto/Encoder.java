package com.poorlytrainedape.crypto;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Encoder {

	private static final String FROM = "crypto.from";
	private static final String TO = "crypto.to";

	public static class EncodingMapper extends Mapper<LongWritable, Text, LongWritable, Text> {		

		private Map<Character, Character> encodingKey = new HashMap<Character, Character>();

		private String encode(String text) {
			text = text.toLowerCase();
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < text.length(); i++) {
				char c = text.charAt(i);
				if (encodingKey.containsKey(c)) {
					sb.append(encodingKey.get(c));
				} else {
					sb.append(c);
				}
			}
			return sb.toString();
		}

		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String to = conf.get(TO);
			String from = conf.get(FROM);
			for (int i = 0; i < from.length(); i++) {
				encodingKey.put(from.charAt(i), to.charAt(i));
			}
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String encoded = encode(value.toString());
			context.write(key, new Text(encoded));
		}
	}

	public static class EncodingReducer extends Reducer<LongWritable, Text, Text, NullWritable> {

		@Override
		protected void reduce(LongWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(value, NullWritable.get());
			}
		}
		
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 4) {
			System.err.println("Usage: Encoder <from> <to> <input> <output>");
			System.exit(-1);
		}

		Job job = new Job();
		job.setJarByClass(Encoder.class);

		Configuration conf  = job.getConfiguration();
		conf.set(FROM, args[0]);
		conf.set(TO, args[1]);

		FileInputFormat.addInputPath(job, new Path(args[2]));
		FileOutputFormat.setOutputPath(job, new Path(args[3]));

		job.setMapperClass(EncodingMapper.class);
		job.setReducerClass(EncodingReducer.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
