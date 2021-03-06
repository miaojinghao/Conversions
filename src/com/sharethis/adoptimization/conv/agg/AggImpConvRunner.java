package com.sharethis.adoptimization.conv.agg;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.sharethis.adoptimization.conv.common.ReadConf;
import com.sharethis.adoptimization.conv.common.Constants;

public class AggImpConvRunner extends Configured implements Tool {
	private static final Logger logger = Logger.getLogger(Constants.AGG_LOGGER_NAME);
	private static final String[] features = {"1", "2", "3", "4"};
	
	protected void logger_init(String conf_file) {
		PropertyConfigurator.configure(conf_file);
		logger.setLevel(Level.INFO);
	}
	
	public int run(String[] args) throws Exception {
		int n = args.length;
		if (n < 2) {
			System.out.println("Runner and log config files needed.");
			System.exit(-1);
		}
		
		ReadConf rf = new ReadConf(args[0]);
		logger_init(args[1]);
		
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "AggregateImpressionsConversions");
		job.setJarByClass(Class.forName(this.getClass().getName()));
		
		// Set Mapper
		job.setMapperClass(com.sharethis.adoptimization.conv.agg.AggImpConvMapper.class);
		job.setMapOutputKeyClass(Text.class);
				
		// Set Reducer
		job.setReducerClass(com.sharethis.adoptimization.conv.agg.AggImpConvReducer.class);
		job.setNumReduceTasks(Constants.NUM_REDUCER);
				
		// Set input and output key/value classes
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// Set input and output file format
		job.setInputFormatClass(TextInputFormat.class);
		// job.setOutputFormatClass(TextOutputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		
		// Set input file paths
		FileSystem fs = FileSystem.get(conf);
		String convFilePath = rf.get("ConversionsOutputPath", "");
		String impFilePath = rf.get("ImpressionsOutputPath", "");
		if (convFilePath != null && impFilePath != null && !convFilePath.isEmpty() && !impFilePath.isEmpty()) {
			for (int i = 0; i < features.length; i++) {
				String convFile = convFilePath + "/" + features[i];
				String impFile = impFilePath + "/" + features[i];
				if (fs.exists(new Path(convFile)) && fs.exists(new Path(impFile))) {
					FileInputFormat.addInputPath(job, new Path(convFile));
					FileInputFormat.addInputPath(job, new Path(impFile));
				}
			}
		}
		
		// Set output file path
		String output_path = rf.get("AggOutputPath", "");
		if (output_path != null && !output_path.isEmpty()) {
			logger.info("Getting output path: " + output_path);
			if (fs.exists(new Path(output_path)))
				fs.delete(new Path(output_path), true);
			FileOutputFormat.setOutputPath(job, new Path(output_path));
		}
		else {
			logger.error("Output path is not specified.");
			return -1;
		}
		
		return job.waitForCompletion(true) ? 1 : 0;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		AggImpConvRunner runner = new AggImpConvRunner();
		args = new GenericOptionsParser(conf, args).getRemainingArgs();
		int exitCode = ToolRunner.run(conf, runner, args);
		System.exit(exitCode);
	}
}