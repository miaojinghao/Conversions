package com.sharethis.adoptimization.conv.impression;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.conv.common.Constants;
import com.sharethis.adoptimization.conv.common.STDateUtils;

public class CollectImpressionsMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Text MapperKey = new Text();
	private Text MapperVal = new Text();
	private static final Logger logger = Logger.getLogger(Constants.COLLECT_IMP_LOGGER_NAME);
	private HashSet<String> hs;
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
	
	@Override
	protected void setup(Context context) throws IllegalArgumentException, IOException, InterruptedException {
		logger.info("Collect Impressions Mapper Starts.");
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
	}
	
	protected void map_out(String key, String value, Context context) throws IOException, InterruptedException {
		MapperKey.set(key);
		MapperVal.set("IMP" + "\t" + value);
		context.write(MapperKey, MapperVal);
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		HashMap<String, String> hm = new HashMap<String, String>();
		
		// Read input data stream into a hashmap
		StringTokenizer st = new StringTokenizer(value.toString(), "\t");
		int index = 0;
		while (st.hasMoreTokens() && index < fields.length) {
			String item = st.nextToken();
			if (item.isEmpty() || item.equalsIgnoreCase("unknown") || item.equalsIgnoreCase("null"))
				item = "unknown";
			hm.put(fields[index], item);
			index++;
		}
		
		// Get AdGroup
		if (hm.containsKey("adgroup_id") && hs.contains(hm.get("adgroup_id"))) {
			map_out("AdGroup", hm.get("adgroup_id") + "\t" + String.valueOf(1.0), context);
		
			// Get Browser
			if (hm.containsKey("browser"))
				map_out("Browser", hm.get("browser") + "\t" + String.valueOf(1.0), context);
		
			// Get OS
			if (hm.containsKey("os"))
				map_out("OS", hm.get("os") + "\t" + String.valueOf(1.0), context);
				
		
			// Get Hour
			if (hm.containsKey("timestamp")) {
				Matcher m_hour = p_hour.matcher(STDateUtils.format(hm.get("timestamp")));
				if (m_hour.find() && m_hour.groupCount() > 0)
					map_out("Hour", m_hour.group(1) + "\t" + String.valueOf(1.0), context);
			}
		
			// Get State and DMA
			if (hm.containsKey("geo_location")) {
				String[] str_geo = hm.get("geo_location").split("\\|");
				if (str_geo.length >= 3) {
					map_out("State", str_geo[0] + "\t" + String.valueOf(1.0), context);
					map_out("DMA", str_geo[2] + "\t" + String.valueOf(1.0), context);
				}
				else {
					map_out("State", "null" + "\t" + String.valueOf(1.0), context);
					map_out("DMA", "null" + "\t" + String.valueOf(1.0), context);
				}
			}
		
			// Get Domain
			if (hm.containsKey("domain"))
				map_out("Domain", hm.get("domain") + "\t" + String.valueOf(1.0), context);
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		logger.info("Collect Conversions Mapper Completed.");
	}
}