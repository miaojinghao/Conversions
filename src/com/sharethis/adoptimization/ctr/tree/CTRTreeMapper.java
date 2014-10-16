package com.sharethis.adoptimization.ctr.tree;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.conv.common.Constants;

public class CTRTreeMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Text MapperKey = new Text();
	private Text MapperVal = new Text();
	private static final Logger logger = Logger.getLogger(Constants.CTR_TREE_LOGGER_NAME);
	private HashSet<String> hs;
	private HashMap<String, String> hm_hour_group;
	private Pattern p_hour = Pattern.compile("(\\d\\d?):.+?");
	
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
	
	private String get_item(String input, String flag) {
		String[] items = input.split("\\|");
		List<String> temp = new ArrayList<String>();
		for (int i = 0; i < items.length; i++) {
			if (!items[i].isEmpty())
				temp.add(items[i]);
		}
		
		String str_id = "";
		int n = temp.size();
		if (n <= 0) return "";
		else {
			if (flag.equals("rand")) {
				Random rand = new Random();
				int rndIndex = rand.nextInt(n);
				if (rndIndex < n) {
					String[] str_temp = temp.get(rndIndex).split(",");
					if (str_temp.length == 2)
						str_id = str_temp[0];
				}
			}
			else if (flag.equals("max")) {
				double max = 0.0;
				for (String each_temp: temp) {
					String[] str_temp = each_temp.split(",");
					try {
						if (str_temp.length == 2 && max <= Double.parseDouble(str_temp[1])) {
							max = Double.parseDouble(str_temp[1]);
							str_id = str_temp[0];
						}
					} catch (Exception e) {
						logger.error("Invalid weight: " + e.toString());
					}
				}
			}
		}	
		return str_id;
	}
	
	@Override
	protected void setup(Context context) throws IllegalArgumentException, IOException, InterruptedException {
		logger.info("CTR Tree Mapper Starts.");
		
		// Set up adgroups from pixels
		Configuration conf = context.getConfiguration();
		String str_pixels = conf.get("pixels");
		hs = new HashSet<String>();
		if (str_pixels != null && (!str_pixels.isEmpty())) {
			StringTokenizer st = new StringTokenizer(str_pixels, ",");
			while (st.hasMoreTokens()) {
				String[] str_pixel_adg = st.nextToken().split("\\|");
				if (str_pixel_adg.length == 2 && !hs.contains(str_pixel_adg[1])) 
					hs.add(str_pixel_adg[1]);
			}
		}
		
		// Set up hour groups
		hm_hour_group = new HashMap<String, String>();
		hm_hour_group.put("07", "1");
		hm_hour_group.put("08", "1");
		hm_hour_group.put("09", "1");
		hm_hour_group.put("10", "1");
		hm_hour_group.put("11", "1");
		hm_hour_group.put("12", "1");
		hm_hour_group.put("13", "1");
		hm_hour_group.put("14", "2");
		hm_hour_group.put("15", "2");
		hm_hour_group.put("16", "2");
		hm_hour_group.put("17", "2");
		hm_hour_group.put("18", "3");
		hm_hour_group.put("19", "3");
		hm_hour_group.put("20", "3");
		hm_hour_group.put("21", "3");
		hm_hour_group.put("22", "3");
		hm_hour_group.put("23", "3");
		hm_hour_group.put("00", "3");
		hm_hour_group.put("01", "4");
		hm_hour_group.put("02", "4");
		hm_hour_group.put("03", "4");
		hm_hour_group.put("04", "4");
		hm_hour_group.put("05", "4");
		hm_hour_group.put("06", "4");
	}
	
	protected void map_out(String key, String value, Context context) throws IOException, InterruptedException {
		MapperKey.set(key);
		MapperVal.set(value);
		context.write(MapperKey, MapperVal);
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		HashMap<String, String> hm = new HashMap<String, String>();
		
		// Read input data stream into a hashmap
		StringTokenizer st = new StringTokenizer(value.toString(), "\t");
		int index = 0;
		while (st.hasMoreTokens() && index < fields.length) {
			hm.put(fields[index], st.nextToken());
			index++;
		}
		
		// Get AdGroup
		if (hs.isEmpty() || (!hs.isEmpty() && hm.containsKey("adgroup_id") && hs.contains(hm.get("adgroup_id")))) {
			// AdGroup
			String str_adg = hm.get("adgroup_id");
			
			String str_campaign = "";
			if (hm.containsKey("campaign_id"))
				str_campaign = hm.get("campaign_id");

			if (!str_adg.isEmpty() && !str_campaign.isEmpty()) {
			
				// Hour
				String str_hour = "null";
				if (hm.containsKey("date")) {
					Matcher m_hour = p_hour.matcher(hm.get("date"));
					if (m_hour.find() && m_hour.groupCount() > 0)
						str_hour = m_hour.group(1);
					if (str_hour.isEmpty() || str_hour.equalsIgnoreCase("null"))
						str_hour = "null";
					if (hm_hour_group.containsKey(str_hour))
						str_hour = hm_hour_group.get(str_hour);
				}
			
				// State
				String str_state = "";
				if (hm.containsKey("geo_location")) {
					String[] str_geo = hm.get("geo_location").split("\\|");
					if (str_geo.length >= 3) 
						str_state = str_geo[0];
					if (str_state.isEmpty() || str_state.equalsIgnoreCase("null"))
						str_state = "null";
				}
			
				/*
				// Browser
				String str_browser = "null";
				if (hm.containsKey("browser"))
					str_browser = hm.get("browser");
				if (str_browser.isEmpty() || str_browser.equalsIgnoreCase("null"))
					str_browser = "null";
			
				// OS
				String str_os = "null";
				if (hm.containsKey("os"))
					str_os = hm.get("os");
				if (str_os.isEmpty() || str_os.equalsIgnoreCase("unknown"))
					str_os = "null";
			
				// Domain
				String str_domain = "null";
				if (hm.containsKey("domain"))
					str_domain = hm.get("domain");
				if (str_domain.isEmpty() || str_domain.equalsIgnoreCase("null") || str_domain.equalsIgnoreCase("unknown"))
					str_domain = "null";
				*/
				
				// Viewability
				String str_view = "";
				if (hm.containsKey("visibility"))
					str_view = hm.get("visibility");
				if (str_view.isEmpty())
					str_view = "NO_DETECTION";
				
				// User segment
				String str_user = "";
				if (hm.containsKey("user_seg_list"))
					str_user = get_item(hm.get("user_seg_list"), "rand");
				if (str_user.isEmpty())
					str_user = "null";
				
				String str_vert = "";
				if (hm.containsKey("verticle_list"))
					str_vert = get_item(hm.get("verticle_list"), "max");
				if (str_vert.isEmpty())
					str_user = "null";
			
				// Click
				String str_click = "0";
				if (hm.containsKey("click_flag"))
					str_click = hm.get("click_flag");
				if (str_click.isEmpty() || str_click.equalsIgnoreCase("null") || !str_click.equals("1"))
					str_click = "0";
			
				// Mapper output
				MapperKey.set(str_hour);
				MapperVal.set(str_state + "\t" + str_view + "\t" + str_vert + "\t" + str_campaign + "\t" + str_click);
				context.write(MapperKey, MapperVal);
			}
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		logger.info("CTR Tree Mapper Completed.");
	}
}