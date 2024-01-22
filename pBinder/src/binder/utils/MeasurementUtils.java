package binder.utils;

public class MeasurementUtils {
	
	public static long sizeOf64(String s) {
		long bytes = 64 + 2L * s.length();
		
		bytes = (long)(8 * Math.ceil(bytes / 8));
		
		return bytes;
	}
}
