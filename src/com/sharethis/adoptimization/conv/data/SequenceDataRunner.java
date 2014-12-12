package com.sharethis.adoptimization.conv.data;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.sharethis.adoptimization.conv.common.ReadConf;
import com.sharethis.adoptimization.conv.common.Constants;
import com.sharethis.adoptimization.conv.common.STDateUtils;

public class SequenceDataRunner extends Configured implements Tool {
	private static final Logger logger = Logger.getLogger(Constants.SEQ_DATA_LOGGER_NAME);
	
	protected void logger_init(String conf_file) {
		PropertyConfigurator.configure(conf_file);
		logger.setLevel(Level.INFO);
	}
	
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		
		// Command line parameters
		String str_config = "";
		String str_log = "";
		
		int n = args.length;
		if (n < 2) {
			System.out.println("Runner and log config files needed.");
			System.exit(-1);
		}
		else {
			str_config = args[0];
			str_log = args[1];
		}
		
		ReadConf rf = new ReadConf(str_config);
		logger_init(str_log);
		
		conf.set("pixels", rf.get("Pixels", ""));
		Job job = Job.getInstance(conf, "Sequence Data");
		job.setJarByClass(Class.forName(this.getClass().getName()));
		
		// Set Mapper
		job.setMapperClass(com.sharethis.adoptimization.conv.data.SequenceDataMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
				
		// Set Reducer
		job.setReducerClass(com.sharethis.adoptimization.conv.data.SequenceDataReducer.class);
		job.setNumReduceTasks(Constants.NUM_REDUCER);
				
		// Set input and output key/value classes
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// Set input and output file format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// Set input file paths
		int nDays = 0;
		try {
			nDays = Integer.parseInt(rf.get("Days", "1"));
			logger.info("Getting number of test data days: " + String.valueOf(nDays));
		} catch (NullPointerException e) {
			logger.error("Null input number of days." + e.toString());
			return -1;
		} catch (NumberFormatException e) {
			logger.error("Invalid input number of days. " + e.toString());
			return -1;
		}
		
		String str_startdate = rf.get("StartDate", "");
		if (!str_startdate.equals("")) {
			conf.set("start_date", str_startdate);
			logger.info("Getting start date: " + str_startdate);
		}
		else {
			logger.error("Invalid input file start date.");
			return -1;
		}
		
		while (nDays > 0) {
			for (int i = 0; i < 24; i++) {
				String hour = String.format("%02d", i);
				// String fileName = "/projects/clickthroughrate/prod/camp_data_hourly/" + str_startdate + hour + "/all_data_hourly";
				String fileName = "s3n://sharethis-insights-backup/camp_data_hourly/" + str_startdate + hour + "/all_data_hourly";
				Path p = new Path(fileName);
				FileSystem fs = p.getFileSystem(conf);
				if (fs.exists(p)) 
					FileInputFormat.addInputPath(job, p);
				// fileName = "/projects/campaign_analytics/prod/retarg/" + str_startdate + hour + "/data";
				fileName = "s3n://sharethis-campaign-analytics/retarg/" + str_startdate + hour + "/data";
				p = new Path(fileName);
				fs = p.getFileSystem(conf);
				if (fs.exists(p)) 
					FileInputFormat.addInputPath(job, p);
			}
			nDays--;
			str_startdate = STDateUtils.getNextDay(str_startdate);
		}
		
		// Set output file path
		String output_path = rf.get("OutputPath", "");
		if (output_path != null && !output_path.isEmpty()) {
			logger.info("Getting output path: " + output_path);
			Path p = new Path(output_path);
			FileSystem fs = p.getFileSystem(conf);
			if (fs.exists(p))
				fs.delete(p, true);
			FileOutputFormat.setOutputPath(job, p);
		}
		else {
			logger.error("Output path is not specified.");
			return -1;
		}
		
		return job.waitForCompletion(true) ? 1 : 0;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		SequenceDataRunner runner = new SequenceDataRunner();
		args = new GenericOptionsParser(conf, args).getRemainingArgs();
		int exitCode = ToolRunner.run(conf, runner, args);
		System.exit(exitCode);
	}
}