package com.sharethis.adoptimization.conv.collection;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.conv.common.Constants;

public class CollectConversionsReducer extends Reducer<Text, Text, Text, Text> {
	private MultipleOutputs<Text, Text> mos;
	private static final Logger logger = Logger.getLogger(Constants.COLLECT_CONV_LOGGER_NAME);
	
	@Override
	protected void setup(Context context) throws IllegalArgumentException, IOException, InterruptedException {
		logger.info("Collect Conversions Reducer Starts.");
		mos = new MultipleOutputs<Text, Text>(context);
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		HashMap<String, Float> hm = new HashMap<String, Float>();
		
		for (Text val: values) {
			String[] tokens = val.toString().split("\t");
			if (tokens.length == 5) {
				String hmKey = tokens[0] + "\t" + tokens[1] + "\t" + tokens[2] + "\t" + tokens[3];
				try {
					if (hm.containsKey(hmKey)) {				
						float count = hm.get(hmKey).floatValue() + Float.parseFloat(tokens[4]);
						hm.put(hmKey, new Float(count));
					}
					else
						hm.put(hmKey, new Float(Float.parseFloat(tokens[4])));
				} catch (Exception e) {
					logger.error("Invalid float number format: " + tokens[4] + " [" + e.toString() + "]");
				}
			}
		}
		
		for (String k: hm.keySet()) {
			mos.write(new Text(key.toString() + "\t" + k), new Text("CONV" + "\t" + String.valueOf(hm.get(k))), key.toString() + "/part");
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
		logger.info("Collect Conversions Reducer Completed.");
	}
}