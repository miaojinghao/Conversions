package com.sharethis.adoptimization.conv.common;

public class ConvUtils {
	public static String join(String sep, String[] items, int start, int end) {
		String ret = "";
		for (int i = start; i <= end - 1; i++)
			ret += (items[i] + sep);
		ret += items[end];
		return ret;
	}
}