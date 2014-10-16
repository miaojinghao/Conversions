package com.sharethis.adoptimization.conv.collection;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
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
	private Pattern p_dma = Pattern.compile("\"dma\":\"(.+?)\"");
	private Pattern p_url = Pattern.compile("\"url\":\"(.+?)\"");
	private Pattern p_st = Pattern.compile("\"st\":\"(.+?)\"");
	
	@Override
	protected void setup(Context context) throws IllegalArgumentException, IOException, InterruptedException {
		logger.info("Collect Conversions Mapper Starts.");
		Configuration conf = context.getConfiguration();
		String str_pixels = conf.get("pixels");
		hm = new HashMap<String, List<String>>();
		if (str_pixels != null && (!str_pixels.isEmpty())) {
			StringTokenizer st = new StringTokenizer(str_pixels, ",");
			while (st.hasMoreTokens()) {
				String[] str_pixel_adg = st.nextToken().split("\\|");
				if (str_pixel_adg.length == 2) {
					List<String> adgroups;
					if (hm.containsKey(str_pixel_adg[0]))
						adgroups = hm.get(str_pixel_adg[0]);
					else
						adgroups = new ArrayList<String>();
					if (adgroups != null) {
						adgroups.add(str_pixel_adg[1]);
						hm.put(str_pixel_adg[0], adgroups);
					}
				}
			}
		}
	}
	
	protected void map_out(String seq, Pattern p, String keyText, Context context) throws IOException, InterruptedException {
		String value = "";
		Matcher m = p.matcher(seq);
		if (m.find() && m.groupCount() > 0)
			value = m.group(1);
		if (value.isEmpty() || value.equalsIgnoreCase("unknown") || value.equalsIgnoreCase("null"))
			value = "unknown";
		MapperKey.set(keyText);
		MapperVal.set("CONV" + "\t" + value + "\t" + String.valueOf(1.0));
		context.write(MapperKey, MapperVal);
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
			Matcher m_cmpn = p_cmpn.matcher(str_seq);
			if (m_cmpn.find() && m_cmpn.groupCount() > 0 && hm.containsKey(m_cmpn.group(1))) {
				// Get adgroup
				List<String> adgroups = hm.get(m_cmpn.group(1));
				int n = adgroups.size();
				if (n > 0) {
					for (String adgroup: adgroups) {
						if (adgroup.isEmpty() || adgroup.equalsIgnoreCase("unknown") || adgroup.equalsIgnoreCase("null"))
							adgroup = "unknown";
						MapperKey.set("AdGroup");
						MapperVal.set("CONV" + "\t" + adgroup + "\t" + String.valueOf(1.0f / ((float) n)));
						context.write(MapperKey, MapperVal);
					}
				}
				
				// Get user agent and OS
				String str_browser = "";
				String str_os = "";
				Matcher m_ua = p_ua.matcher(str_seq);
				if (m_ua.find() && m_ua.groupCount() > 0) {
					String user_agent = m_ua.group(1);
					if (user_agent != null && !user_agent.isEmpty()) {
						UserAgent ua = UserAgent.parseUserAgentString(user_agent);
						if (ua != null) {
							Browser browser = ua.getBrowser();
							OperatingSystem os = ua.getOperatingSystem();
							if (browser != null)
								str_browser = browser.getName().replaceAll("\\s", "");
							if (os != null)
								str_os = os.getName().replaceAll("\\s", "");
						}
					}
				}
				
				if (str_browser.isEmpty() || str_browser.equalsIgnoreCase("unknown") || str_browser.equalsIgnoreCase("null"))
					str_browser = "unknown";
				MapperKey.set("Browser");
				MapperVal.set("CONV" + "\t" + str_browser + "\t" + String.valueOf(1.0));
				context.write(MapperKey, MapperVal);
				if (str_os.isEmpty() || str_os.equalsIgnoreCase("unknown") || str_os.equalsIgnoreCase("null"))
					str_os = "unknown";
				MapperKey.set("OS");
				MapperVal.set("CONV" + "\t" + str_os + "\t" + String.valueOf(1.0));
				context.write(MapperKey, MapperVal);
				
				// Get hours
				map_out(str_seq, p_hour, "Hour", context);
				
				// Get dma
				map_out(str_seq, p_dma, "DMA", context);
				
				// Get state
				map_out(str_seq, p_st, "State", context);
				
				// Get domain ID
				String domain = "";
				Matcher m_url = p_url.matcher(str_seq);
				if (m_url.find() && m_url.groupCount() > 0) {
					try {
						URI uri = new URI(m_url.group(1));
						if (uri != null) {
							domain = uri.getHost();
							if (domain == null)
								domain = "";
						}
					} catch (URISyntaxException e) {
						logger.error("Invalid Conversion URL: " + e.toString());
						domain = "";
					}
				}
				if (domain.isEmpty() || domain.equalsIgnoreCase("unknown") || domain.equalsIgnoreCase("null"))
					domain = "unknown";
				MapperKey.set("Domain");
				MapperVal.set("CONV" + "\t" + domain + "\t" + String.valueOf(1.0));
				context.write(MapperKey, MapperVal);
			}
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		logger.info("Collect Conversions Mapper Completed.");
	}
}