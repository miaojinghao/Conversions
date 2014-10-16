package com.sharethis.adoptimization.conv.impression;

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
import com.sharethis.adoptimization.conv.common.STDateUtils;
import com.sharethis.adoptimization.conv.common.Constants;

public class CollectImpressionsRunner extends Configured implements Tool {
	private static final Logger logger = Logger.getLogger(Constants.COLLECT_IMP_LOGGER_NAME);
	
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
		conf.set("pixels", rf.get("Pixels", ""));
		Job job = Job.getInstance(conf, "CollectImpressions");
		job.setJarByClass(Class.forName(this.getClass().getName()));
		
		// Set Mapper
		job.setMapperClass(com.sharethis.adoptimization.conv.impression.CollectImpressionsMapper.class);
		job.setMapOutputKeyClass(Text.class);
				
		// Set Reducer
		job.setReducerClass(com.sharethis.adoptimization.conv.impression.CollectImpressionsReducer.class);
		job.setNumReduceTasks(Constants.NUM_REDUCER);
				
		// Set input and output key/value classes
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// Set input and output file format
		job.setInputFormatClass(TextInputFormat.class);
		// job.setOutputFormatClass(TextOutputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		
		int nDays = 0;
		try {
			nDays = Integer.parseInt(rf.get("Days", "1"));
			logger.info("Getting number of input days: " + String.valueOf(nDays));
		} catch (NullPointerException e) {
			logger.error("Null input number of days." + e.toString());
			return -1;
		} catch (NumberFormatException e) {
			logger.error("Invalid input number of days. " + e.toString());
			return -1;
		}
		
		String str_startdate = rf.get("InputFileDate", "");
		if (!str_startdate.equals("")) {
			conf.set("start_date", str_startdate);
			logger.info("Getting start date: " + str_startdate);
		}
		else {
			logger.error("Invalid input file start date.");
			return -1;
		}
		
		// Set input file paths
		FileSystem fs = FileSystem.get(conf);
		while (nDays > 0) {
			for (int i = 0; i < 24; i++) {
				String hour = String.format("%02d", i);
				String fileName = "/projects/clickthroughrate/prod/camp_data_hourly/" + str_startdate + hour + "/all_data_hourly";
				if (fs.exists(new Path(fileName))) 
					FileInputFormat.addInputPath(job, new Path(fileName));
			}
			nDays--;
			str_startdate = STDateUtils.getNextDay(str_startdate);
		}
		
		// Set output file path
		String output_path = rf.get("ImpressionsOutputPath", "");
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
		CollectImpressionsRunner runner = new CollectImpressionsRunner();
		args = new GenericOptionsParser(conf, args).getRemainingArgs();
		int exitCode = ToolRunner.run(conf, runner, args);
		System.exit(exitCode);
	}
}