package rdtrc.logManager;

import java.util.HashMap;
import java.util.LinkedList;

import rdtrc.structures.LockEntity;
import rdtrc.structures.LogEntry;

public class LogManager {
	private static LogManager instance = null;
	private HashMap<Long, LogEntry> scratchPaper;
	private HashMap<Long, LinkedList<Long>> transactionLSNs;
	
	long latestLogSequenceNumber = 0;

	private LogManager() {
		scratchPaper = new HashMap<Long, LogEntry>();
		transactionLSNs = new HashMap<Long, LinkedList<Long>>();
	}

	public static LogManager getInstance() {
		if (instance == null) {
			instance = new LogManager();
		}
		return instance;
	}
	
	public long logEntry(LockEntity currentEntity) {
		latestLogSequenceNumber++;
		
		LogEntry logEntry = new LogEntry(currentEntity, -1, -1, -1);
		scratchPaper.put(latestLogSequenceNumber, logEntry);
		
		LinkedList<Long> lsns = transactionLSNs.get(currentEntity.getTransactionTimestamp());
		
		if(lsns == null) {
			lsns = new LinkedList<Long>();
			lsns.add(latestLogSequenceNumber);
			transactionLSNs.put(currentEntity.getTransactionTimestamp(), lsns);
		} else {
			transactionLSNs.get(currentEntity.getTransactionTimestamp()).addLast(latestLogSequenceNumber);
		}
				
		return latestLogSequenceNumber;
	}
	
	public void updateScratchPaper(long lsn, LogEntry entry)
	{
		scratchPaper.put(lsn , entry);
	}
	
	public LogEntry getLogEntry(long lsn)
	{
		return scratchPaper.get(lsn);
	}
	
	public LinkedList<Long> getLSNPerTransaction(long transactionTimestamp) {
		return transactionLSNs.get(transactionTimestamp);
	}
}
