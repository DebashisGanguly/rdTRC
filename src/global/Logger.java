package rdtrc.global;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class Logger {
	private static Logger instance = null;
	
	public static Logger getInstance() {
		if(instance == null) {
			instance = new Logger();
		}
		return instance;
	}
	
	public void WriteLog(String message) {
		String logFileName = Constant.fileFolder + "/" + Constant.logFile;
		
		File logFile = new File(logFileName);
		
		if(!logFile.exists()) {
			try {
				logFile.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		if(!logFile.canWrite()){
			return;
		}

		try {
			PrintWriter output = new PrintWriter(new FileWriter(logFileName, true));
		
			output.println(message);
			output.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
