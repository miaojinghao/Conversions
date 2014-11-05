package com.sharethis.adoptimization.conv.collection;

import java.io.IOException;
// import java.net.URI;
// import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import eu.bitwalker.useragentutils.Browser;
import eu.bitwalker.useragentutils.OperatingSystem;
import eu.bitwalker.useragentutils.UserAgent;

import com.sharethis.adoptimization.conv.common.Constants;

public class CollectConversionsMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Text MapperKey = new Text();
	private Text MapperVal = new Text();
	private static final Logger logger = Logger.getLogger(Constants.COLLECT_CONV_LOGGER_NAME);
	private HashMap<String, List<String>> hm;
	private Pattern p_cmpn = Pattern.compile("\"cmpn\":\"(\\w+?)\"");
	private Pattern p_ua = Pattern.compile("\"brwsos\":\"(.+?)\"");
	private Pattern p_hour = Pattern.compile("\"date\":\".+? (\\d+?):.+?\"");
	// private Pattern p_dma = Pattern.compile("\"dma\":\"(.+?)\"");
	// private Pattern p_url = Pattern.compile("\"url\":\"(.+?)\"");
	private Pattern p_st = Pattern.compile("\"st\":\"(.+?)\"");
	
	@Override
	protected void setup(Context context) throws IllegalArgumentException, IOException, InterruptedException {
		logger.info("Collect Conversions Mapper Starts.");
		
		hm = new HashMap<String, List<String>>();
		Configuration conf = context.getConfiguration();
		String str_pixels = conf.get("pixels");
		if (str_pixels != null && (!str_pixels.isEmpty())) {
			StringTokenizer st = new StringTokenizer(str_pixels, ",");
			while (st.hasMoreTokens()) {
				/* token = pixel|ID where ID = CampaignID or AdGroupID*/
				String[] tokens = st.nextToken().split("\\|");
				if (tokens.length == 2) {
					List<String> ids;
					if (hm.containsKey(tokens[0]))
						ids = hm.get(tokens[0]);
					else
						ids = new ArrayList<String>();
					if (ids != null) {
						ids.add(tokens[1]);
						hm.put(tokens[0], ids);
					}
				}
			}
		}
	}
	
	protected String get_pattern(String seq, Pattern p) {
		String value = "";
		Matcher m = p.matcher(seq);
		if (m.find() && m.groupCount() > 0)
			value = m.group(1);
		if (value.isEmpty() || value.equalsIgnoreCase("unknown") || value.equalsIgnoreCase("null"))
			value = "-";
		return value;
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		StringTokenizer tok = new StringTokenizer(line, "\t");
		String str_cookie = "";
		String str_seq = "";
		if (tok.hasMoreTokens())
			str_cookie = tok.nextToken();
		if (tok.hasMoreTokens())
			str_seq = tok.nextToken();
		if (str_cookie.length() > 0 && str_seq.length() > 0) {
			// Get IDs
			Matcher m_cmpn = p_cmpn.matcher(str_seq);
			if (m_cmpn.find() && m_cmpn.groupCount() > 0 && hm.containsKey(m_cmpn.group(1))) {
				List<String> ids = hm.get(m_cmpn.group(1));
				int n = ids.size();
			
				// Get hour group
				String str_hour = get_pattern(str_seq, p_hour);
				String str_hour_group = Constants.HM_HOUR_GROUP.get(str_hour);
				if (str_hour_group == null || str_hour_group.isEmpty() || str_hour_group.equalsIgnoreCase("unknown") || str_hour_group.equalsIgnoreCase("null"))
					str_hour_group = "-";
			
				// Get state
				String str_state = get_pattern(str_seq, p_st);
			
				// Get DMA
				// String str_dma = get_pattern(str_seq, p_dma);
				
				// Get OS and browser
				String str_browser = "";
				String str_os = "";
				String str_ua = get_pattern(str_seq, p_ua);
				if (!str_ua.isEmpty()) {
					UserAgent ua = UserAgent.parseUserAgentString(str_ua);
					if (ua != null) {
						Browser browser = ua.getBrowser();
						OperatingSystem os = ua.getOperatingSystem();
						if (browser != null)
							str_browser = browser.getName().replaceAll("[^a-zA-Z]", "");
						if (os != null)
							str_os = os.getName().replaceAll("\\s|(\\(.+?\\))|\\.[a-zA-Z0-9]+?", "");
					}
				}
				if (str_browser.isEmpty() || str_browser.equalsIgnoreCase("unknown") || str_browser.equalsIgnoreCase("null"))
					str_browser = "-";
				if (str_os.isEmpty() || str_os.equalsIgnoreCase("unknown") || str_os.equalsIgnoreCase("null"))
					str_os = "-";
			
				// Mapper output
				if (n > 0) {
					for (String id: ids) {
						MapperKey.set(str_hour_group);
						MapperVal.set(str_state + "\t" + str_os + "\t" + str_browser + "\t" + id + "\t" + String.valueOf(1.0f / n));
						context.write(MapperKey, MapperVal);
					}
				}
			}
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		logger.info("Collect Conversions Mapper Completed.");
	}
}