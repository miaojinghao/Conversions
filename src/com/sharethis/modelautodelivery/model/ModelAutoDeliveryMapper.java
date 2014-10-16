package com.sharethis.modelautodelivery.model;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.sharethis.modelautodelivery.common.dateUtils;
import com.sharethis.modelautodelivery.common.modelConstants;

public class ModelAutoDeliveryMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Text adgroup = new Text();
	private Text seq = new Text();
	private HashMap<String, String> hmAdGroups;
	private HashSet<String> hsModelID;
	private int nStartDate;
	private int nEndDate;
	private static final Logger logger = Logger.getLogger(modelConstants.LOGGER_NAME);
	
	private static final String[] fields = {
		"bid", "wp_bucket", "ad_slot_id", "visibility", "geo_location", 
		"google_id", "setting_id", "user_seg_list", "verticle_list", "date", 
		"timestamp", "campaign_name", "jid", "campaign_id", "adgroup_id", 
		"creative_id", "domain", "deal_id", "ip", "user_agent", 
		"cookie", "service_type", "st_campaign_id", "st_adgroup_id", "st_creative_id", 
		"click_flag","imp_flag", "click_timestamp", "min_bid", "platform_type", 
		"geo_target_id", "mobile", "model_id", "model_score", "gid_age", 
		"audience_id", "browser", "os", "device", "creative_size", 
		"sqi", "err_flag", "flag", "app_id", "carrier_id", 
		"device_type", "is_app", "is_initial_request", "platform", "screen_orientation", 
		"device_id", "location", "device_make", "device_model", "device_id_type", 
		"seller_id"
	};
	
	@Override
	protected void setup(Context context) throws IllegalArgumentException, IOException, InterruptedException {
		logger.info("Model Threshold Auto Adjustment Mapper Started.");
		Configuration conf = context.getConfiguration();
		hmAdGroups = new HashMap<String, String>();
		
		String str_adgroups = conf.get("adgroups");
		if (str_adgroups != null) {
			StringTokenizer st = new StringTokenizer(str_adgroups, ",");
			while (st.hasMoreTokens()) {
				String[] entries = st.nextToken().split("\\|");
				if (entries.length == 2)
					hmAdGroups.put(entries[0], entries[1]);
			}
		}
		
		hsModelID = new HashSet<String>();
		String strModelID = conf.get("model_id");
		if (strModelID != null && !strModelID.isEmpty()) {
			StringTokenizer st = new StringTokenizer(strModelID, ",");
			while (st.hasMoreTokens()) {
				String str_model_id = st.nextToken();
				if (str_model_id != null && !str_model_id.isEmpty())
					hsModelID.add(str_model_id);
			}
		}
		
		String strStartDate = conf.get("start_date");
		if (strStartDate == null)
			strStartDate = "0";
		
		String strEndDate = conf.get("end_date");
		if (strEndDate == null)
			strEndDate = strStartDate;
		
		try {
			nStartDate = Integer.parseInt(strStartDate);
			nEndDate = Integer.parseInt(strEndDate);
		} catch (NullPointerException e) {
			logger.error("Model Auto Delivery Mapper Null start date: " + strStartDate + "|" + strEndDate + " [" + e.toString() + "]");
			nStartDate = 0;
			nEndDate = 0;
		} catch (NumberFormatException e) {
			logger.error("Model Auto Delivery Mapper Invalid start date format: " + strStartDate + "|" + strEndDate + " [" + e.toString() + "]");
			nStartDate = 0;
			nEndDate = 0;
		}
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		HashMap<String, String> hm = new HashMap<String, String>();
		
		StringTokenizer st = new StringTokenizer(value.toString(), "\t");
		int index = 0;
		while (st.hasMoreTokens() && index < fields.length) {
			hm.put(fields[index], st.nextToken());
			index++;
		}
		
		if (hsModelID != null && hmAdGroups != null && 
			hm.containsKey("model_id") && hsModelID.contains(hm.get("model_id")) &&
			hm.containsKey("adgroup_id") && hmAdGroups.containsKey(hm.get("adgroup_id")) && 
			hm.containsKey("model_score") && hm.containsKey("click_flag") && hm.containsKey("timestamp")) {
			String str_date = dateUtils.format(hm.get("timestamp"));
			int day_index = str_date.indexOf(' ');
			if (day_index > 0) {
				String date = str_date.substring(0, day_index);
				try {
					int data_date = Integer.parseInt(date);
					int start_date = Integer.parseInt(hmAdGroups.get(hm.get("adgroup_id")));
					if (data_date >= start_date && data_date >= nStartDate && data_date <= nEndDate) {
						adgroup.set(hm.get("adgroup_id"));
						seq.set(date + "\t" + hm.get("model_score") + "\t" + hm.get("click_flag"));
						context.write(adgroup, seq);
					}
				} catch (NullPointerException e) {
					logger.error("Model Auto Delivery Mapper Null string: val [" + e.toString() + "]");
				} catch  (NumberFormatException e) {
					logger.error("Model Auto Delivery Mapper Invalid data format: val [" + e.toString() + "]");
				}
			}
		}
		
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		logger.info("Model Threshold Auto Adjustment Mapper Completed.");
	}
	
}