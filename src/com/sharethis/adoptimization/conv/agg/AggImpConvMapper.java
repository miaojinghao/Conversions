package com.sharethis.adoptimization.conv.agg;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.sharethis.adoptimization.conv.common.Constants;
import com.sharethis.adoptimization.conv.common.ConvUtils;

public class AggImpConvMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Text MapperKey = new Text();
	private Text MapperVal = new Text();
	private static final Logger logger = Logger.getLogger(Constants.AGG_LOGGER_NAME);

	@Override
	protected void setup(Context context) throws IllegalArgumentException, IOException, InterruptedException {
		logger.info("Aggregate Impressions and Conversions Mapper Starts.");
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// Input stream format: Name <tab> Label <tab> CONV/IMP <tab> Value
		// Mapper output: 
		// Key: Name <tab> Label
		// Value: CONV/IMP <tab> Value
		String[] items= value.toString().split("\t");
		if (items.length == 7) {
			MapperKey.set(ConvUtils.join("\t", items, 0, 4));
			MapperVal.set(ConvUtils.join("\t", items, 5, 6));
			context.write(MapperKey, MapperVal);
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		logger.info("Aggregate Impressions and Conversions Mapper Completed.");
	}
}