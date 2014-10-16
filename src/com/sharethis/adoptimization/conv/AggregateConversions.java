package com.sharethis.adoptimization.conv;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

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

public class AggregateConversions extends Configured implements Tool {
	
	// Mapper class
	public static class ConvMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text cookie = new Text();
		private Text seq = new Text();
		private HashSet<String> hs;
		private HashSet<String> hs2;
		
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
		
		@Override
		protected void setup(Context context) throws IllegalArgumentException, IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			
			// Campaign data
			String str_cmpn = conf.get("campaign");
			hs = new HashSet<String>();
			if (str_cmpn == null)
				str_cmpn = "";
			StringTokenizer tok = new StringTokenizer(str_cmpn, ",");
			while (tok.hasMoreTokens())
				hs.add(tok.nextToken());
			
			// Pixel data
			String str_pixel = conf.get("pixel");
			hs2 = new HashSet<String>();
			if (str_pixel == null)
				str_pixel = "";
			tok = new StringTokenizer(str_pixel, ",");
			while (tok.hasMoreTokens())
				hs2.add(tok.nextToken());
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tok = new StringTokenizer(line, "\t");
			int nTokens = tok.countTokens();
			String first_token = "";
			if(tok.hasMoreTokens())
				first_token = tok.nextToken();
			
			String str_seq = "";
			
			// Impression Data
			if (first_token.length() != 32) {
				int index = 1;
				int nFields = fields.length;
				HashMap<String, String> map = new HashMap<String, String>();
				map.put(fields[0], first_token);
				while (tok.hasMoreTokens() && index < nFields && index < nTokens) {
					map.put(fields[index], tok.nextToken());
					index++;
				}
				if (!map.get("cookie").equals("") && map.containsKey("campaign_name") && map.containsKey("date") && map.containsKey("jid") &&
					map.containsKey("visibility") && map.containsKey("setting_id") && map.containsKey("domain") && 
					map.containsKey("user_seg_list") && map.containsKey("verticle_list") && map.containsKey("creative_id") && 
					map.containsKey("sqi") && map.containsKey("browser") && map.containsKey("os") && map.containsKey("device") &&
					map.containsKey("click_flag") && hs.contains(map.get("campaign_name"))) {
					cookie.set(map.get("cookie"));
					str_seq = map.get("date") + "\t" + map.get("jid") + "\t" + map.get("visibility") + "\t" + map.get("setting_id") + 
							  "\t" + map.get("domain") + "\t" + map.get("user_seg_list") + "\t" + map.get("verticle_list") + 
							  "\t" + map.get("creative_id") + "\t" + map.get("sqi") + "\t" + map.get("browser") + 
							  "\t" + map.get("os") + "\t" + map.get("device") + "\t" + map.get("click_flag");
					seq.set(str_seq);
					context.write(cookie,  seq);
				}
			}
			
			// Conversion Data
			else {
				cookie.set(first_token);
				
				String date_regexp = "\"date\":\"([\\d :]+?)\"";
				String cmpn_regexp = "\"cmpn\":\"(\\w+?)\"";
				Pattern p1 = Pattern.compile(date_regexp);
				Pattern p2 = Pattern.compile(cmpn_regexp);
				
				while (tok.hasMoreTokens()) {
					String s = tok.nextToken();
					Matcher m1 = p1.matcher(s);
					Matcher m2 = p2.matcher(s);
					if (m1.find() && m2.find() && m1.groupCount() > 0 && m2.groupCount() > 0) {
						String date = m1.group(1);
						String cmpn = m2.group(1);
						if (hs2.contains(cmpn)) {
							str_seq = date + "\t" + cmpn;
							seq.set(str_seq);
							context.write(cookie, seq);
						}
					}
				}
			}
		}
	}
	
	// Reducer class
	public static class ConvReducer extends Reducer<Text, Text, Text, Text> {
		private Text jid = new Text();
		private Text seq = new Text();
		private String attribution = "";
		
		@Override
		protected void setup(Context context) throws IllegalArgumentException, IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			attribution = conf.get("attribution");
			if (attribution == null)
				attribution = "";
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			TreeMap<String, String> tm = new TreeMap<String, String>();
			for (Text val: values) {
				String s = val.toString();
				int i = s.indexOf("\t");
				if (i > 0) {
					String ts = s.substring(0, i);
					tm.put(ts,  s);
				}
			}
			String prev = "";
			String prev_date = "";
			for (String ts: tm.keySet()) {
				String s = tm.get(ts);
				StringTokenizer tok = new StringTokenizer(s, "\t");
				int nTokens = tok.countTokens();
				int i = s.indexOf("\t");
				if (i < 0) continue;
				if (nTokens == 13) {
					if (!prev.equals("") && prev.indexOf("\t") > 0) {
						jid.set(prev.substring(0, prev.indexOf("\t")));
						seq.set(prev_date + "\t" + prev.substring(prev.indexOf("\t") + 1) + "\t0\t0");
						context.write(jid, seq);
					}
					prev_date = s.substring(0, i);
					prev = s.substring(i + 1);
				}
				else if (nTokens == 2) {
					// String cmpn = s.substring(i + 1);
					if (attribution.equals("PostImpression")) {
						if (!prev.equals("") && prev.indexOf("\t") > 0) {
							jid.set(prev.substring(0, prev.indexOf("\t")));
							seq.set(prev_date + "\t" + prev.substring(prev.indexOf("\t") + 1) + "\t" + s);
							context.write(jid, seq);
						}
						prev = "";
						prev_date = "";
					}
					else if (attribution.equals("PostClick")) {
						int j = prev.lastIndexOf("\t");
						if (j < 0 || j == prev.length() - 1) continue;
						String isClick = prev.substring(j + 1);
						if (isClick.equals("1") && !prev.equals("") && prev.indexOf("\t") > 0) {
							jid.set(prev.substring(0, prev.indexOf("\t")));
							seq.set(prev_date + "\t" + prev.substring(prev.indexOf("\t") + 1) + "\t" + s);
							context.write(jid, seq);
							prev = "";
							prev_date = "";
						}
					}
				}
			}
			if (!prev.equals("") && prev.indexOf("\t") > 0) {
				jid.set(prev.substring(0, prev.indexOf("\t")));
				seq.set(prev_date + "\t" + prev.substring(prev.indexOf("\t") + 1) + "\t0\t0");
				context.write(jid, seq);
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
		int exitCode = ToolRunner.run(new AggregateConversions(), args);
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
		conf.set("campaign", rf.get("Campaign", ""));
		conf.set("pixel", rf.get("Pixel", ""));
		conf.set("attribution", rf.get("Attribution", ""));
		Job job = Job.getInstance(conf, "Aggregate Conversions");
		
		job.setJarByClass(AggregateConversions.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(ConvMapper.class);
		job.setReducerClass(ConvReducer.class);
		
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
			nDays = 1;
		}
		
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
		
		String additional_file_name = rf.get("AdditionalFile", "");
		if (!additional_file_name.equals("")) {
			if (fs.exists(new Path(additional_file_name)))
				FileInputFormat.addInputPath(job, new Path(additional_file_name));
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