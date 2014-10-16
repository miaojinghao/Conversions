package com.sharethis.modelautodelivery.common;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.commons.lang3.time.DateUtils;

public class dateUtils {
	
	public static String getNextDay(String dateString) {
		try {
			DateFormat df = new SimpleDateFormat("yyyyMMdd");
			Date inDate = df.parse(dateString);
			Date retDate = DateUtils.addDays(inDate, 1);
			return df.format(retDate);
		} catch (IllegalArgumentException e) {
			return "";
		} catch (ParseException e) {
			return "";
		}
	}
	
	public static String getPreviousDay(String dateString) {
		try {
			DateFormat df = new SimpleDateFormat("yyyyMMdd");
			Date inDate = df.parse(dateString);
			Date retDate = DateUtils.addDays(inDate, -1);
			return df.format(retDate);
		} catch (IllegalArgumentException e) {
			return "";
		} catch (ParseException e) {
			return "";
		}
	}
	
	public static String getAddDays(String dateString, int nDays) {
		if (nDays == 0)
			return dateString;
		
		try {
			DateFormat df = new SimpleDateFormat("yyyyMMdd");
			Date inDate = df.parse(dateString);
			Date retDate = DateUtils.addDays(inDate, nDays);
			return df.format(retDate);
		} catch (IllegalArgumentException e) {
			return "";
		} catch (ParseException e) {
			return "";
		}
	}
	
	public static String format(String ms) {
		try {
			long unix_ms = Long.parseLong(ms);
			Date dt = new Date(unix_ms);
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
			sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
			return sdf.format(dt);
		} catch (NumberFormatException e) {
			return "";
		}
	}
}