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
	private HashMap<String, String> hmCat;
	private Pattern p_hour = Pattern.compile("(\\d\\d?):.+?");
	
	@Override
	protected void setup(Context context) throws IllegalArgumentException, IOException, InterruptedException {
		logger.info("Collect Impressions Mapper Starts.");
		Configuration conf = context.getConfiguration();
		String str_pixels = conf.get("pixels");
		hs = new HashSet<String>();
		hmCat = new HashMap<String, String>();
		if (str_pixels != null && (!str_pixels.isEmpty())) {
			StringTokenizer st = new StringTokenizer(str_pixels, ",");
			while (st.hasMoreTokens()) {
				String[] tokens = st.nextToken().split("\\|");
				if (tokens.length == 3) {
					if (!hs.contains(tokens[1])) 
						hs.add(tokens[1]);
					hmCat.put(tokens[1], tokens[2]);
				}
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
		while (st.hasMoreTokens() && index < Constants.FIELDS.length) {
			String item = st.nextToken();
			if (item.isEmpty() || item.equalsIgnoreCase("unknown") || item.equalsIgnoreCase("null"))
				item = "-";
			hm.put(Constants.FIELDS[index], item);
			index++;
		}
		
		// Get AdGroup
		if (hm.containsKey("campaign_id") && hs.contains(hm.get("campaign_id"))) {
			
			// Get ID
			String str_id = "-";
			if (hmCat.containsKey(hm.get("campaign_id")))
				str_id = hmCat.get(hm.get("campaign_id"));
			if (str_id.isEmpty() || str_id.equalsIgnoreCase("unknown") || str_id.equalsIgnoreCase("null"))
				str_id = "-";
			
			// Get browser
			String str_browser = "-";
			if (hm.containsKey("browser"))
				str_browser = hm.get("browser").replaceAll("[^a-zA-Z]", "");
			if (str_browser.isEmpty() || str_browser.equalsIgnoreCase("unknown") || str_browser.equalsIgnoreCase("null"))
				str_browser = "-";
			
			// Get OS
			String str_os = "-";
			if (hm.containsKey("os"))
				str_os = hm.get("os").replaceAll("\\s|(\\(.+?\\))|\\.[a-zA-Z0-9]+?", "");
			if (str_os.isEmpty() || str_os.equalsIgnoreCase("unknown") || str_os.equalsIgnoreCase("null"))
				str_os = "-";
			
			// Get hour group
			String str_hour = "-";
			if (hm.containsKey("timestamp")) {
				Matcher m_hour = p_hour.matcher(STDateUtils.format(hm.get("timestamp")));
				if (m_hour.find() && m_hour.groupCount() > 0)
					str_hour = m_hour.group(1);
			}
			String str_hour_group = "1";
			if (Constants.HM_HOUR_GROUP.containsKey(str_hour))
				str_hour_group = Constants.HM_HOUR_GROUP.get(str_hour);
			if (str_hour_group.isEmpty() || str_hour_group.equalsIgnoreCase("unknown") || str_hour_group.equalsIgnoreCase("null"))
				str_hour_group = "-";
			
			// Get state and DMA
			String str_state = "-";
			String str_dma = "-";
			if (hm.containsKey("geo_location")) {
				String[] str_geo = hm.get("geo_location").split("\\|");
				if (str_geo.length >= 3) {
					str_state = str_geo[0];
					str_dma = str_geo[2];
				}
			}
			if (str_state.isEmpty() || str_state.equalsIgnoreCase("unknown") || str_state.equalsIgnoreCase("null"))
				str_state = "-";
			if (str_dma.isEmpty() || str_dma.equalsIgnoreCase("unknown") || str_dma.equalsIgnoreCase("null"))
				str_dma = "-";
			
			// Get domain
			// String str_domain = "";
			// if (hm.containsKey("domain"))
			//	str_domain = hm.get("domain");
		
			// Output
			MapperKey.set(str_hour_group);
			MapperVal.set(str_state + "\t" + str_os + "\t" + str_browser + "\t" + str_id + "\t" + String.valueOf(1.0f));
			context.write(MapperKey, MapperVal);
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		logger.info("Collect Conversions Mapper Completed.");
	}
}