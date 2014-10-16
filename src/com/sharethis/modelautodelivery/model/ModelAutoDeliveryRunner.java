package com.sharethis.modelautodelivery.model;

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

import com.sharethis.modelautodelivery.common.readConf;
import com.sharethis.modelautodelivery.common.dateUtils;
import com.sharethis.modelautodelivery.common.modelConstants;

public class ModelAutoDeliveryRunner extends Configured implements Tool {
	private static final Logger logger = Logger.getLogger(modelConstants.LOGGER_NAME);
	
	public ModelAutoDeliveryRunner(Configuration conf) {
		super(conf);
	}
	
	protected void logger_init(String conf_file) {
		PropertyConfigurator.configure(conf_file);
		logger.setLevel(Level.INFO);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		int n = args.length;
		if (n < 2) {
			logger.error("Model and logger conf files needed.");
			return -1;
		}
		
		Configuration conf = this.getConf();
		logger_init(args[1]);
		
		// Read command line parameters
		readConf rf = new readConf(args[0]);
		logger.info("Reading conf file: " + args[0]);
		
		// Set up parameters to be passed to mapper
		String str_adgroups = "";
		String str_model_id = "";
		String str_startdate = "";
		
		str_adgroups = rf.get("AdGroups", "");
		if (!str_adgroups.equals("")) {
			conf.set("adgroups", str_adgroups);
			logger.info("Getting adgroups: " + str_adgroups);
		}
		else {
			logger.error("Invalid adgroups.");
			return -1;
		}
		str_model_id = rf.get("Model_ID", "");
		if (!str_model_id.equals("")) {
			conf.set("model_id", str_model_id);
			logger.info("Getting model_id: " + str_model_id);
		}
		else {
			logger.error("Invalid model IDs.");
			return -1;
		}
		str_startdate = rf.get("InputFileDate", "");
		if (!str_startdate.equals("")) {
			conf.set("start_date", str_startdate);
			logger.info("Getting start date: " + str_startdate);
		}
		else {
			logger.error("Invalid input file start date.");
			return -1;
		}
		
		int nDays = 0;
		try {
			nDays = Integer.parseInt(rf.get("Days", "1"));
			String str_enddate = dateUtils.getAddDays(str_startdate, nDays - 1);
			conf.set("end_date", str_enddate);
			logger.info("Getting number of input dates: " + String.valueOf(nDays));
			logger.info("Getting end date: " + str_enddate);
		} catch (NullPointerException e) {
			logger.error("Null input number of days." + e.toString());
			return -1;
		} catch (NumberFormatException e) {
			logger.error("Invalid input number of days. " + e.toString());
			return -1;
		}
		
		// Set Job
		Job job = Job.getInstance(conf, "ModelAutoDelivery");
		job.setJarByClass(Class.forName(this.getClass().getName()));
		
		// Set Mapper
		job.setMapperClass(com.sharethis.modelautodelivery.model.ModelAutoDeliveryMapper.class);
		job.setMapOutputKeyClass(Text.class);
				
		// Set Reducer
		job.setReducerClass(com.sharethis.modelautodelivery.model.ModelAutoDeliveryReducer.class);
		job.setNumReduceTasks(modelConstants.NUM_REDUCER);
				
		// Set input and output key/value classes
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// Set input and output file paths
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Set input path		
		FileSystem fs = FileSystem.get(conf);
		while (nDays > 0) {
			for (int i = 0; i < 24; i++) {
				String hour = String.format("%02d", i);
				String fileName = "/projects/clickthroughrate/prod/camp_data_hourly/" + str_startdate + hour + "/all_data_hourly";
				if (fs.exists(new Path(fileName))) 
					FileInputFormat.addInputPath(job, new Path(fileName));
			}
			nDays--;
			str_startdate = dateUtils.getNextDay(str_startdate);
		}
		
		// Set output path
		String output_path = rf.get("OutputPath", "");
		if (output_path != null && !output_path.isEmpty()) {
			logger.info("Getting output path: " + output_path);
			if (fs.exists(new Path(output_path))) 
				fs.delete(new Path(output_path), true);
			FileOutputFormat.setOutputPath(job, new Path(rf.get("OutputPath", "")));
		}
		else {
			logger.error("Output path is not specified.");
			return -1;
		}
		
		return job.waitForCompletion(true) ? 1 : 0;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		ModelAutoDeliveryRunner runner = new ModelAutoDeliveryRunner(conf);
		args = new GenericOptionsParser(conf, args).getRemainingArgs();
		int exitCode = ToolRunner.run(conf, runner, args);
		System.exit(exitCode);
	}	
}