package com.sharethis.modelautodelivery.model;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import com.sharethis.modelautodelivery.common.modelConstants;

public class ModelAutoDeliveryReducer extends Reducer<Text, Text, Text, Text> {
	private static final int DAYINC = 10000;
	private static final Logger logger = Logger.getLogger(modelConstants.LOGGER_NAME);
	
	private void bucket_output(TreeMap<Integer, List<Integer>> tm, Context context, Text key, int n) throws IOException, InterruptedException {
		int total_imps = 0;
		int total_clicks = 0;
		int inc = DAYINC;
		int i = 1;
		Integer bucket = new Integer(0);
		
		for (Integer b: tm.keySet()) {
			bucket = b;
			List<Integer> al = tm.get(b);
			if (total_imps >= inc * i) {
				double ctr = total_imps > 0 ? ((double) total_clicks) / ((double) total_imps) : -1.0;
				String seq = String.format("%d\t%d\t%d\t%.6f\t%d", b.intValue(), total_imps, total_clicks, ctr, n);
				context.write(key, new Text(seq));
				i = ((int) (total_imps / inc)) + 1;
			}
			total_imps += al.get(0).intValue();
			total_clicks += al.get(1).intValue();
		}
		if (total_imps > inc * (i - 1)) {
			double ctr = total_imps > 0 ? ((double) total_clicks) / ((double) total_imps) : -1.0;
			String seq = String.format("%d\t%d\t%d\t%.6f\t%d", bucket.intValue(), total_imps, total_clicks, ctr, n);
			context.write(key, new Text(seq));
		}		
	}
	
	@Override
	protected void setup(Context context) throws IllegalArgumentException, IOException, InterruptedException {
		logger.info("Model Threshold Auto Adjustment Reducer Started.");
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		TreeMap<Integer, List<Integer>> tm = new TreeMap<Integer, List<Integer>>(Collections.reverseOrder());
		HashSet<String> hs = new HashSet<String>();
		
		for (Text val: values) {
			// Val: model_id, model_score, click_flag
			double model_score = 0.0;
			int click_flag = 0;
			String date = "";

			try {
				StringTokenizer st = new StringTokenizer(val.toString(), "\t");
				if (st.hasMoreTokens())
					date = st.nextToken();
				if (st.hasMoreTokens())
					model_score = Double.parseDouble(st.nextToken());
				if (st.hasMoreTokens())
					click_flag = Integer.parseInt(st.nextToken());
				
				// Bucketize traffic and update numbers
				Integer score_bucket = new Integer((int) (model_score * 1000000));
				if (tm.containsKey(score_bucket)) {
					List<Integer> al = tm.get(score_bucket);
					al.set(0, new Integer(al.get(0).intValue() + 1));
					al.set(1, new Integer(al.get(1).intValue() + click_flag));
					tm.put(score_bucket, al);
				}
				else {
					List<Integer> al = new ArrayList<Integer>(2);
					al.add(new Integer(1));
					al.add(new Integer(click_flag));
					tm.put(score_bucket, al);
				}
				
				if (!hs.contains(date))
					hs.add(date);
				
			} catch (NullPointerException e) {
				logger.error("Model Auto Delivery Reducer Null string: val [" + e.toString() + "]");
				continue;
			} catch (NumberFormatException e) {
				logger.error("Model Auto Delivery Reducer Invalid data format: val [" + e.toString() + "]");
				continue;
			}
		}
		if (tm.size() > 0)
			bucket_output(tm, context, key, hs.size());	
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		logger.info("Model Threshold Auto Adjustment Reducer Completed.");
	}
	
}