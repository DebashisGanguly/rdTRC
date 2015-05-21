package rdtrc.global;

import java.util.regex.Pattern;
import rdtrc.structures.ConcurrentReadingMode;

public final class Constant {
	public static int pageSize = 512;
	public static String configurationFileName = "rdtrc.conf";
	public static String bufferSizeConfigurationItem = "BufferSize:";
	public static int idSize = 4;
	public static int clientNameSize = 16;
	public static int phoneNumberSize = 12;
	public static String id = "Id";
	public static String clientName = "ClientName";
	public static String phoneNumber = "PhoneNumber";
	public static String index = "Index";
	public static String fileFolder = "RDTRC";
	public static String retrieveByIdCommand = "R";
	public static String insertCommand = "I";
	public static String deleteCommand = "D";
	public static String groupByAreaCodeCountCommand = "G";
	public static String retrieveByAreaCodeCommand = "M";
	public static String commitCommand = "C";
	public static String abortCommand = "A";
	public static String beginCommand = "B";
	public static String scriptExtension = "rdtrc";
	public static String logFile = "executionLogger.log";
	public static Pattern scriptParsingPattern = Pattern.compile("\\b([a-zA-Z]{1})\\b(([\\s]+\\b[\\w]+\\b(([\\s]+[\\d]+)|([\\s]+\\([\\d]+[\\s]*,[\\s]*[a-zA-Z]+[\\s]*,[\\s]*\\d{3}-\\d{3}-\\d{4}\\))|))|)");
	public static Pattern insertArgumentParsingPattern = Pattern.compile("[\\d]+[\\s]*,[\\s]*[a-zA-Z]+[\\s]*,[\\s]*\\d{3}-\\d{3}-\\d{4}");
	public static String secondaryIndex = "sIndex";

	public static String concurrentReadingModeConfigurationItem = "ConcurrentReadingMode:";
	public static String randomSeedConfigurationItem = "RandomSeed:";
	
	public static int bufferSize;
	public static long randomSeed;
	public static ConcurrentReadingMode readingMode;
}
