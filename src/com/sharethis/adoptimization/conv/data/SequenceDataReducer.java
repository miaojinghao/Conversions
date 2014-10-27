package com.sharethis.adoptimization.conv.data;

import java.io.IOException;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.conv.common.Constants;
import com.sharethis.adoptimization.conv.common.STDateUtils;

public class SequenceDataReducer extends Reducer<Text, Text, Text, Text> {
	private static final Logger logger = Logger.getLogger(Constants.SEQ_DATA_LOGGER_NAME);	
	private Pattern p_date = Pattern.compile("\"date\":\"(.+?)\"");
	private Pattern p_hour = Pattern.compile("\\s(\\d\\d?):.+?");
	private Pattern p_cmpn = Pattern.compile("\"cmpn\":\"(\\w+?)\"");
	private Text ReducerKey = new Text();
	private Text ReducerVal = new Text();
	
	@Override
	protected void setup(Context context) throws IllegalArgumentException, IOException, InterruptedException {
		logger.info("Sequence Impressions and Conversions Reducer Starts.");
	}
	
	protected String get_pattern(String seq, Pattern p) {
		String value = "";
		Matcher m = p.matcher(seq);
		if (m.find() && m.groupCount() > 0)
			value = m.group(1);
		if (value == null || value.isEmpty() || value.equalsIgnoreCase("unknown") || value.equalsIgnoreCase("null"))
			value = "unknown";
		return value;
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		TreeMap<String, String> tm = new TreeMap<String, String>();
		for (Text val: values) {
			String[] items = val.toString().split("\t");
			int n = items.length;
			if (n <= 1)
				continue;
			
			String str_tag = items[0];
			if (str_tag.equals("CONV")) {
				for (int i = 0; i < n; i++) {
					String date = get_pattern(items[i], p_date);
					String cmpn = get_pattern(items[i], p_cmpn);
					tm.put(date, cmpn);
				}
			}
			else if (str_tag.equals("IMP")) {
				HashMap<String, String> hm = new HashMap<String, String>();
				for (int i = 1; i < n && i < Constants.FIELDS.length + 1; i++) {
					String item = items[i];
					if (item.isEmpty() || item.equalsIgnoreCase("unknown") ||item.equalsIgnoreCase("null"))
						item = "unknown";
					hm.put(Constants.FIELDS[i - 1], item);
				}
				if (hm.containsKey("timestamp")) {
					String date = STDateUtils.format(hm.get("timestamp"));
					
					// Get ID
					String str_id = "";
					if (hm.containsKey("campaign_id"))
						str_id = hm.get("campaign_id");
					if (str_id == null || str_id.isEmpty() || str_id.equalsIgnoreCase("unknown") || str_id.equalsIgnoreCase("null"))
						str_id = "unknown";
					
					// Get hour group
					String str_hour = "";
					if (hm.containsKey("timestamp")) {
						Matcher m_hour = p_hour.matcher(date);
						if (m_hour.find() && m_hour.groupCount() > 0)
							str_hour = m_hour.group(1);
					}
					
					String str_hour_group = "unknown";
					if (Constants.HM_HOUR_GROUP.containsKey(str_hour))
						str_hour_group = Constants.HM_HOUR_GROUP.get(str_hour);
					
					// Get State and DMA
					String str_state = "";
					String str_dma = "";
					if (hm.containsKey("geo_location")) {
						String[] str_geo = hm.get("geo_location").split("\\|");
						if (str_geo.length >= 3) {
							str_state = str_geo[0];
							str_dma = str_geo[2];
						}
					}
					if (str_state.isEmpty() || str_state.equalsIgnoreCase("unknown") || str_state.equalsIgnoreCase("null"))
						str_state = "unknown";
					if (str_dma.isEmpty() || str_dma.equalsIgnoreCase("unknown") || str_dma.equalsIgnoreCase("null"))
						str_dma = "unknown";
					
					// Get browser
					String str_browser = "";
					if (hm.containsKey("browser"))
						str_browser = hm.get("browser").replaceAll("[^a-zA-Z]", "");
					
					// Get OS
					String str_os = "";
					if (hm.containsKey("os"))
						str_os = hm.get("os").replaceAll("\\s|(\\(.+?\\))|\\.[a-zA-Z0-9]+?", "");
					
					// Get domain
					// String str_domain = "";
					// if (hm.containsKey("domain"))
					//	str_domain = hm.get("domain");
					
					String str_imp = str_hour_group + "\t" + str_state + "\t" + str_os + "\t" + str_browser + "\t" + str_id;
					tm.put(date, str_imp);
				}
			}
		}
		
		String prev = "";
		for (String key_date: tm.keySet()) {
			String record = tm.get(key_date);
			String[] tokens = record.split("\t");
			String conv = "0";
			// Conversions
			if (tokens.length == 1) 
				conv = "1";
			// Impressions
			else
				conv = "0";

			if (!prev.isEmpty()) {
				ReducerKey = key;
				ReducerVal.set(prev + "\t" + conv);
				context.write(ReducerKey, ReducerVal);
			}
			
			if (tokens.length == 1) 
				prev = "";
			else
				prev = record;		
		}
		
		if (!prev.isEmpty()) {
			ReducerKey = key;
			ReducerVal.set(prev + "\t" + "0");
			context.write(ReducerKey, ReducerVal);
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		logger.info("Sequence Impressions and Conversions Completed.");
	}
}