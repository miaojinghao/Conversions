package com.sharethis.adoptimization.conv;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class GenerateAggFiles extends Configured implements Tool {
	
	// Mapper class
	public static class ConvMapper extends Mapper<LongWritable, Text, Text, Text> {
		private static final String[] fields = {
			"jid", "impression_time", "viewable_vr", "setting_vr", "domain_vr", "user_seg_vr", "verticle_vr", "creative_vr", 
			"sqi_vr", "browser_vr", "os_vr", "device_vr", "click_flag", "conversion_time", "conversions"
		};
		
		private String get_item(String input, String flag) {
			StringTokenizer tok = new StringTokenizer(input, "|");
			int n = tok.countTokens();
			if (n <= 0) return "";
			else {
				Random rand = new Random();
				String str_id = "";
				if (flag.equals("random")) {
					int rndIndex = rand.nextInt(n);
					int i = 0;
					while (tok.hasMoreTokens()) {
						String item = tok.nextToken();
						if (i == rndIndex) {
							StringTokenizer t = new StringTokenizer(item, ",");
							str_id = t.nextToken();
							break;
						}
						i++;
					}
				}
				else if (flag.equals("max")) {
					double max = 0.0;
					while (tok.hasMoreTokens()) {
						String item = tok.nextToken();
						if (!item.equals("")) {
							StringTokenizer t = new StringTokenizer(item, ",");
							if (t.countTokens() < 2) continue;
							String temp_str_id = t.nextToken();
							String temp_str_score = t.nextToken();
							if (max <= Double.parseDouble(temp_str_score)) {
								str_id = temp_str_id;
								max = Double.parseDouble(temp_str_score);
							}
						}
					}
				}
				return str_id;
			}
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tok = new StringTokenizer(line, "\t");
			if (tok.countTokens() >= 12) {
				List<String> l = new ArrayList<String>();
				while (tok.hasMoreTokens())
					l.add(tok.nextToken());
				int i = 0;
				while (i < l.size() - 1 && i < fields.length) {
					if (i == 3 || i == 4)
						context.write(new Text(fields[i]), new Text(l.get(i) + "\t" + l.get(13)));
					else if (i == 5) {
						context.write(new Text(fields[i]), new Text(get_item(l.get(i), "random") + "\t" + l.get(13)));
					}
					else if (i == 6) {
						context.write(new Text(fields[i]), new Text(get_item(l.get(i), "max") + "\t" + l.get(13)));
					}
					i++;
				}
			}
			
		}
	}
	
	// Reducer class
	public static class ConvReducer extends Reducer<Text, Text, Text, Text> {
		private MultipleOutputs<Text, Text> mos;
		
		public void setup(Context context) throws IOException, InterruptedException {
			mos = new MultipleOutputs<Text, Text>(context);
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			Map<String, List<Integer>> m = new HashMap<String, List<Integer>>();
			for (Text val: values) {
				StringTokenizer tok = new StringTokenizer(val.toString(), "\t");
				if (tok.countTokens() < 2)
					continue;
				String k = "";
				if (tok.hasMoreTokens())
					k = tok.nextToken();
				if (tok.hasMoreTokens()) {
					if (m.containsKey(k)) {
						int imp = m.get(k).get(0).intValue() + 1;
						int clk = m.get(k).get(1).intValue();
						if (!tok.nextToken().equals("0"))
							clk = clk + 1;
						m.get(k).set(0, new Integer(imp));
						m.get(k).set(1, new Integer(clk));
					}
					else {
						List<Integer> l = new ArrayList<Integer>();
						l.add(new Integer(1));
						if (!tok.nextToken().equals("0"))
							l.add(new Integer(1));
						else
							l.add(new Integer(0));
						if (!k.equals(""))
							m.put(k, l);
					}
				}
			}
			int total_imp = 0;
			int total_clk = 0;
			for (String k: m.keySet()) {
				int imp = m.get(k).get(0).intValue();
				int clk = m.get(k).get(1).intValue();
				total_imp += imp;
				total_clk += clk;
				if (imp > 0) {
					double ctr = (double) clk / (double) imp;
					String v = String.valueOf(ctr);
					mos.write(new Text("ADX" + "\t" + k), new Text(v), key.toString() + "/part");
				}
			}
			if (total_imp > 0) {
				double ctr = (double) total_clk / (double) total_imp;
				String v = String.valueOf(ctr);
				mos.write(new Text("ADX" + "\t" + "-99"), new Text(v), key.toString() + "/part");
			}
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		args = new GenericOptionsParser(conf, args).getRemainingArgs();
		int exitCode = ToolRunner.run(conf, new GenerateAggFiles(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "Generate rule data");

		int n = args.length;
		if (n < 2) {
			System.out.println("At least two parameters needed: input_path and output_path.");
			System.exit(-1);
		}
		
		job.setJarByClass(Class.forName(this.getClass().getName()));
		
		// Set Mapper
		job.setMapperClass(ConvMapper.class);
		job.setMapOutputKeyClass(Text.class);
		
		// Set Reducer
		job.setReducerClass(ConvReducer.class);
		
		// Set input and output key/value classes
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// Set input and output file paths
		job.setInputFormatClass(TextInputFormat.class);
		// job.setOutputFormatClass(TextOutputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		// MultipleOutputs.addNamedOutput(job, "text", TextOutputFormat.class, Text.class, Text.class);
		
		for (int i = 0; i < n - 1; i++)
			FileInputFormat.addInputPath(job, new Path(args[i]));
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(args[n - 1]))) {
			fs.delete(new Path(args[n - 1]), true);
		}
		FileOutputFormat.setOutputPath(job, new Path(args[n - 1]));
		
		return job.waitForCompletion(true) ? 1 : 0;
	}
}