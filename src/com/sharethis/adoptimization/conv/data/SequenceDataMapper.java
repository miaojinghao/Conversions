package com.sharethis.adoptimization.conv.data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.sharethis.adoptimization.conv.common.Constants;

public class SequenceDataMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Text MapperKey = new Text();
	private Text MapperVal = new Text();
	private Pattern p_cmpn = Pattern.compile("\"cmpn\":\"(\\w+?)\"");
	private HashSet<String> hs_ids;
	private HashSet<String> hs_cmpns;
	
	private static final Logger logger = Logger.getLogger(Constants.SEQ_DATA_LOGGER_NAME);

	@Override
	protected void setup(Context context) throws IllegalArgumentException, IOException, InterruptedException {
		logger.info("Sequence Impressions and Conversions Mapper Starts.");
		
		hs_ids = new HashSet<String>();
		hs_cmpns = new HashSet<String>();
		
		Configuration conf = context.getConfiguration();
		String str_pixels = conf.get("pixels");
		if (str_pixels != null && (!str_pixels.isEmpty())) {
			StringTokenizer st = new StringTokenizer(str_pixels, ",");
			while (st.hasMoreTokens()) {
				String[] tokens = st.nextToken().split("\\|");
				if (tokens.length == 3) {
					if (!hs_ids.contains(tokens[1]))
						hs_ids.add(tokens[1]);
					if (!hs_cmpns.contains(tokens[0]))
						hs_cmpns.add(tokens[0]);
				}
			}
		}
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// Input stream are a mixture of campaign hourly data and retargeting data
		String[] items= value.toString().split("\t");
		int n = items.length;
		if (n > 1) {
			String first = items[0];
			
			// If its a retargeting record
			if (first.length() == 32) {
				List<String> val = new ArrayList<String>();				
				for (int i = 1; i < n; i++) {
					Matcher m_cmpn = p_cmpn.matcher(items[i]);
					if (m_cmpn.find() && m_cmpn.groupCount() > 0 && hs_cmpns.contains(m_cmpn.group(1)))
						val.add(items[i]);
				}
				if (val.size() > 0) {
					MapperKey.set(first);
					MapperVal.set("CONV" + "\t" + Joiner.on("\t").join(val));
					context.write(MapperKey, MapperVal);
				}
			}
			// Else it is a campaign hourly data format
			else {
				// Read input data stream
				HashMap<String, String> hm = new HashMap<String, String>();
				for (int i = 0; i < n && i < Constants.FIELDS.length; i++) {
					String item = items[i];
					if (item.isEmpty() || item.equalsIgnoreCase("unknown") || item.equalsIgnoreCase("null"))
						item = "-";
					hm.put(Constants.FIELDS[i], item);
				}
				
				if (hm.containsKey("campaign_id") && hm.containsKey("cookie") && hs_ids.contains(hm.get("campaign_id"))) {
					String cookie_id = hm.get("cookie");
					if (!cookie_id.equals("-")) {
						MapperKey.set(cookie_id);
						MapperVal.set("IMP" + "\t" + value.toString());
						context.write(MapperKey, MapperVal);
					}
				}
			}
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		logger.info("Sequence Impressions and Conversions Mapper Completed.");
	}
}