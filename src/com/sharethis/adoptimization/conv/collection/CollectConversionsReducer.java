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
			String[] str_val_cnt = val.toString().split("\t");
			if (str_val_cnt.length == 3) {
				try {
					if (hm.containsKey(str_val_cnt[1])) {				
						float count = hm.get(str_val_cnt[1]).floatValue() + Float.parseFloat(str_val_cnt[2]);
						hm.put(str_val_cnt[1], new Float(count));
					}
					else
						hm.put(str_val_cnt[1], new Float(Float.parseFloat(str_val_cnt[2])));
				} catch (Exception e) {
					logger.error("Invalid float number format: " + str_val_cnt[2] + " [" + e.toString() + "]");
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