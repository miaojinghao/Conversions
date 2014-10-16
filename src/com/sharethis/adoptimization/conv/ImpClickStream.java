package com.sharethis.adoptimization.conv;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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

import com.sharethis.adoptimization.conv.common.*;

public class ImpClickStream extends Configured implements Tool {
	
	public static class StreamMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text cookie = new Text();
		private Text seq = new Text();
		
		private static final String[] fields = {
			"bid", "wp_bucket", "ad_slot_id", "visibility", "geo_location", "google_id", "setting_id", 
			"user_seg_list", "verticle_list", "date", "timestamp", "campaign_name", "jid", "campaign_id", 
			"adgroup_id", "creative_id", "domain", "deal_id", "ip", "user_agent", "cookie", "service_type", 
			"st_campaign_id", "st_adgroup_id", "st_creative_id", "click_flag","imp_flag", "click_timestamp", 
			"min_bid", "platform_type", "geo_target_id", "mobile", "mobile_id", "mobile_score", 
			"gid_age", "audience_id", "browser", "os", "device", "creative_size", "sqi", "err_flag", 
			"flag", "app_id", "carrier_id", "device_type", "is_app", "is_initial_request", "platform", 
			"screen_orientation", "device_id", "location", "device_make", "device_model", "device_id_type"
		};
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer tok = new StringTokenizer(value.toString(), "\t");
			int index = 0;
			HashMap<String, String> map = new HashMap<String, String>();
			while (tok.hasMoreTokens()) {
				map.put(fields[index], tok.nextToken());
				index ++;
			}
			if (map.containsKey("cookie") && !map.get("cookie").equals("") && map.containsKey("timestamp") &&
				map.containsKey("click_flag") && map.containsKey("click_timestamp") && map.containsKey("campaign_id")) {
				String str_seq = map.get("timestamp") + "\t" + map.get("campaign_id") + "\t" + map.get("click_flag") + "\t" + map.get("click_timestamp");
				cookie.set(map.get("cookie"));
				seq.set(str_seq);
				context.write(cookie, seq);
			}
		}
	}
	
	public static class StreamReducer extends Reducer<Text, Text, Text, Text> {
		private Text seq = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			TreeMap<String, String> tm = new TreeMap<String, String>();
			for (Text val: values) {
				int i = val.toString().indexOf("\t");
				if (i > 0) {
					String ts = val.toString().substring(0, i);
					tm.put(ts, val.toString());
				}				
			}
			
			String str_seq = "";
			for (String ts: tm.keySet()) {
				String s = tm.get(ts);
				str_seq = str_seq + s + "\t";
			}
			if (str_seq.length() > 0) {
				str_seq = str_seq.substring(0, str_seq.length() - 1);
				seq.set(str_seq);
				context.write(key, seq);
			}
		}
	}
	
	public static String getNextDate(String dateString) {
		try {
			DateFormat df = new SimpleDateFormat("yyyyMMdd");
			Date myDate = df.parse(dateString);
			Calendar cal = Calendar.getInstance();
			cal.setTime(myDate);
			cal.add(Calendar.DAY_OF_YEAR, 1);
			return df.format(cal.getTime());
		} catch (ParseException e) {
			return "";
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		args = new GenericOptionsParser(conf, args).getRemainingArgs();
		int exitCode = ToolRunner.run(new ImpClickStream(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();

		int n = args.length;
		if (n < 1) {
			System.out.println("Conf file needed.");
			System.exit(-1);
		}
		
		ReadConf rf = new ReadConf(args[0]);
		Job job = Job.getInstance(conf, "Impression and click streams");
		
		job.setJarByClass(ImpClickStream.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(StreamMapper.class);
		job.setReducerClass(StreamReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// Set input path
		String inDate = rf.get("InputFileDate", "");
		if (inDate.equals(""))
			return -1;
		
		int nDays = 1;
		try {
			nDays = Integer.parseInt(rf.get("Days", "1"));
		} catch (NumberFormatException e) {
			return -1;
		}
		
		// Set input path
		FileSystem fs = FileSystem.get(conf);
		while (nDays > 0) {
			for (int i = 0; i < 24; i++) {
				String hour = String.format("%02d", i);
				String fileName = "/projects/clickthroughrate/prod/camp_data_hourly/" + inDate + hour + "/all_data_hourly";
				if (fs.exists(new Path(fileName))) 
					FileInputFormat.addInputPath(job, new Path(fileName));
			}
			nDays--;
			inDate = getNextDate(inDate);
		}
		
		// Set output path
		if (fs.exists(new Path(rf.get("OutputPath", "")))) {
			fs.delete(new Path(rf.get("OutputPath", "")), true);
		}
		if (!rf.get("OutputPath", "").equals(""))
			FileOutputFormat.setOutputPath(job, new Path(rf.get("OutputPath", "")));
		
		return job.waitForCompletion(true) ? 1 : 0;
	}
}