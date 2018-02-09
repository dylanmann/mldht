package the8472.utils;

public class MathUtils {
	
	public static long roundToNearestMultiple(long num, long multiple) {
	    return multiple * ((num + multiple - 1) / multiple);
	}
	
	public static long ceilDiv(long num, long divisor){
	    return -Math.floorDiv(-num,divisor);
	}

	public static String humanReadableBytes(long bytes) {
		int unit = 1000;
		if (bytes < unit) return bytes + " B";
		int exp = (int) (Math.log(bytes) / Math.log(unit));
		char pre = "kMGTPE".charAt(exp-1);
		return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
	}

}
