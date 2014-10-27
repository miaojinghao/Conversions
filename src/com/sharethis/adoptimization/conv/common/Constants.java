package com.sharethis.adoptimization.conv.common;

import java.util.HashMap;

public class Constants {
	public static final String COLLECT_CONV_LOGGER_NAME = "collect.conversions";
	public static final String COLLECT_IMP_LOGGER_NAME = "collect.impressions";
	public static final String CTR_TREE_LOGGER_NAME = "CTR.decision.tree";
	public static final String AGG_LOGGER_NAME = "agg.imp.conv";
	public static final String SEQ_DATA_LOGGER_NAME = "seq.data";
	public static final int IMP_CTR_THR = 10000;
	public static final int NUM_REDUCER = 100;
	
	public static final HashMap<String, String> HM_HOUR_GROUP;
	static {
		HM_HOUR_GROUP = new HashMap<String, String>();
		HM_HOUR_GROUP.put("07", "1");
		HM_HOUR_GROUP.put("08", "1");
		HM_HOUR_GROUP.put("09", "1");
		HM_HOUR_GROUP.put("10", "1");
		HM_HOUR_GROUP.put("11", "1");
		HM_HOUR_GROUP.put("12", "1");
		HM_HOUR_GROUP.put("13", "1");
		HM_HOUR_GROUP.put("14", "2");
		HM_HOUR_GROUP.put("15", "2");
		HM_HOUR_GROUP.put("16", "2");
		HM_HOUR_GROUP.put("17", "2");
		HM_HOUR_GROUP.put("18", "3");
		HM_HOUR_GROUP.put("19", "3");
		HM_HOUR_GROUP.put("20", "3");
		HM_HOUR_GROUP.put("21", "3");
		HM_HOUR_GROUP.put("22", "3");
		HM_HOUR_GROUP.put("23", "3");
		HM_HOUR_GROUP.put("00", "3");
		HM_HOUR_GROUP.put("01", "4");
		HM_HOUR_GROUP.put("02", "4");
		HM_HOUR_GROUP.put("03", "4");
		HM_HOUR_GROUP.put("04", "4");
		HM_HOUR_GROUP.put("05", "4");
		HM_HOUR_GROUP.put("06", "4");
	};
	
	public static final String[] FIELDS = {
		"bid", "wp_bucket", "ad_slot_id", "visibility", "geo_location", 
		"google_id", "setting_id", "user_seg_list", "verticle_list", "date", 
		"timestamp", "campaign_name", "jid", "campaign_id", "adgroup_id", 
		"creative_id", "domain", "deal_id", "ip", "user_agent", 
		"cookie", "service_type", "st_campaign_id", "st_adgroup_id", "st_creative_id", 
		"click_flag","imp_flag", "click_timestamp", "min_bid", "platform_type", 
		"geo_target_id", "mobile", "model_id", "model_score", "gid_age", 
		"audience_id", "browser", "os", "device", "creative_size", 
		"sqi", "err_flag", "flag", "app_id", "carrier_id", 
		"device_type", "is_app", "is_initial_request", "platform", "screen_orientation", 
		"device_id", "location", "device_make", "device_model", "device_id_type", 
		"seller_id", "geofence_id", "geo_long", "geo_lat"
	};
}