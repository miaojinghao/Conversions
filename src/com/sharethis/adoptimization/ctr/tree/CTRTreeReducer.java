package com.sharethis.adoptimization.ctr.tree;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.sharethis.adoptimization.conv.common.Constants;

public class CTRTreeReducer extends Reducer<Text, Text, Text, Text> {
	private MultipleOutputs<Text, Text> mos;
	private static final Logger logger = Logger.getLogger(Constants.CTR_TREE_LOGGER_NAME);
	
	@Override
	protected void setup(Context context) throws IllegalArgumentException, IOException, InterruptedException {
		logger.info("CTR Tree Reducer Starts.");
		mos = new MultipleOutputs<Text, Text>(context);
	}
	
	/*
	protected void merge(HashMap<String, List<Integer>> hm) {
		for (String key: hm.keySet()) {
			
		}
	}
	*/
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		HashMap<String, List<Integer>> hm = new HashMap<String, List<Integer>>();
		
		String str_hour = key.toString();
		
		for (Text val: values) {
			String[] str_val = val.toString().split("\t");
			if (str_val.length > 1) {
				int n = str_val.length;
				String str_click = str_val[n - 1];
				List<String> temp = new ArrayList<String>();
				temp.add(str_hour);
				for (int i = 0; i < n - 1; i++)
					temp.add(str_val[i]);
				String hm_key = Joiner.on("\t").join(temp);

				List<Integer> hm_val;
				if (hm.containsKey(hm_key)) {
					hm_val = hm.get(hm_key);
					if (hm_val.size() == 2) {
						int imp = hm_val.get(0).intValue() + 1;
						int clk = hm_val.get(1).intValue();
						try {
							clk += Integer.parseInt(str_click);
						} catch (NumberFormatException e) {
							logger.error("Illegal click flag format: " + str_click + " [" + e.toString() + "]");
						}
						hm_val.set(0, new Integer(imp));
						hm_val.set(1, new Integer(clk));
						hm.put(hm_key, hm_val);
					}
				}
				else {
					hm_val = new ArrayList<Integer>(2);
					try {
						hm_val.add(new Integer(1));
						hm_val.add(new Integer(Integer.parseInt(str_click)));
					} catch (NumberFormatException e) {
						logger.error("Illegal click flag format: " + str_click + " [" + e.toString() + "]");
						hm_val.add(new Integer(1));
						hm_val.add(new Integer(0));
					}
					hm.put(hm_key, hm_val);
				}
			}
		}
		
		for (String k: hm.keySet()) {
			List<Integer> val = hm.get(k);
			if (val.size() != 2)
				continue;
			try {
				int imp = val.get(0).intValue();
				int clk = val.get(1).intValue();
				double ctr = imp > 0 ? (double) clk / (double) imp : -1;
				mos.write(new Text(k), new Text(String.valueOf(imp) + "\t" + String.valueOf(clk) + "\t" + String.valueOf(ctr)), key.toString() + "/part");
			} catch (IllegalArgumentException e) {
				logger.error("Illegal Augument Exception: " + e.toString());
			} catch (Exception e) {
				logger.error("Other Exception: " + e.toString());
			}
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
		logger.info("CTR Tree Reducer Completed.");
	}
}