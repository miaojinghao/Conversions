package com.sharethis.adoptimization.conv.agg;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.conv.common.Constants;

public class AggImpConvReducer extends Reducer<Text, Text, Text, Text> {
	private MultipleOutputs<Text, Text> mos;
	private static final Logger logger = Logger.getLogger(Constants.AGG_LOGGER_NAME);
	
	@Override
	protected void setup(Context context) throws IllegalArgumentException, IOException, InterruptedException {
		logger.info("Aggregate Impressions and Conversions Reducer Starts.");
		mos = new MultipleOutputs<Text, Text>(context);
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		float imp = 0.0f;
		float clk = 0.0f;
		
		for (Text val: values) {
			String[] items= val.toString().split("\t");
			if (items.length == 2) {
				try {
					if(items[0].equals("CONV"))
						clk = Float.parseFloat(items[1]);
					else if (items[0].equals("IMP"))
						imp = Float.parseFloat(items[1]);
				} catch (Exception e) {
					logger.error(e.toString());
					clk = 0.0f;
					imp = 0.0f;
				}
			}
		}
		float ctr = imp > 0.0f? clk / imp : -1.0f;
		
		String[] items = key.toString().split("\t");
		if (items.length == 2) {
			String output_path_key = items[0];
			String reducer_val = String.valueOf(imp) + "\t" + String.valueOf(clk) + "\t" + String.valueOf(ctr);
			mos.write(new Text(items[1]), new Text(reducer_val), output_path_key + "/part");
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
		logger.info("Aggregate Impressions and Conversions Reducer Completed.");
	}
}