package com.sharethis.adoptimization.conv.agg;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.conv.common.Constants;

public class AggImpConvReducer extends Reducer<Text, Text, Text, Text> {
	private MultipleOutputs<Text, Text> mos;
	private Text ReducerKey = new Text();
	private Text ReducerVal = new Text();
	private static final Logger logger = Logger.getLogger(Constants.AGG_LOGGER_NAME);
	
	@Override
	protected void setup(Context context) throws IllegalArgumentException, IOException, InterruptedException {
		logger.info("Aggregate Impressions and Conversions Reducer Starts.");
		mos = new MultipleOutputs<Text, Text>(context);
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		float imp = 0.0f;
		float conv = 0.0f;
		
		for (Text val: values) {
			String[] items= val.toString().split("\t");
			if (items.length == 2) {
				try {
					if(items[0].equals("CONV"))
						conv = Float.parseFloat(items[1]);
					else if (items[0].equals("IMP"))
						imp = Float.parseFloat(items[1]);
				} catch (Exception e) {
					logger.error(e.toString());
					conv = 0.0f;
					imp = 0.0f;
				}
			}
		}
		
		if (imp > 0.0f) {
			float ctr = imp > 0.0f? (conv + 1) / (imp + 1000) : -1.0f;
		
			String[] items = key.toString().split("\t");
			if (items.length == 5) {
				String output_path_key = items[0];
				String reducer_val = String.valueOf(imp) + "\t" + String.valueOf(conv) + "\t" + String.format("%.8f", ctr);
				ReducerKey = key;
				ReducerVal.set(reducer_val);
				mos.write(ReducerKey, ReducerVal, output_path_key + "/part");
			}
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
		logger.info("Aggregate Impressions and Conversions Reducer Completed.");
	}
}