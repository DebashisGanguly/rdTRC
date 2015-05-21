package rdtrc.structures;

public class ScriptBookmark {
	
	private int lineNumber;
	private boolean isTransaction;
	private long timestamp;
	private long transactionNumber;
	
	public ScriptBookmark(int lineNumebr, boolean isTransaction, long timestamp, long transactionNumber) {
		this.lineNumber = lineNumebr;
		this.isTransaction = isTransaction;
		this.timestamp = timestamp;
		this.setTransactionNumber(transactionNumber);
	}

	public ScriptBookmark() {
		super();
		this.lineNumber = -1;
		this.isTransaction = false;
		this.timestamp = -1;
		this.setTransactionNumber(-1);
	}
	
	public ScriptBookmark(ScriptBookmark s) {
		this.lineNumber = s.lineNumber;
		this.isTransaction = s.isTransaction;
		this.timestamp = s.timestamp;
		this.setTransactionNumber(s.getTransactionNumber());
	}

	public int getLineNumber() {
		return lineNumber;
	}
	
	public void setLineNumber(int lineNumebr) {
		this.lineNumber = lineNumebr;
	}
	
	public boolean isTransaction() {
		return isTransaction;
	}
	
	public void setTransaction(boolean isTransaction) {
		this.isTransaction = isTransaction;
	}
	
	public long getTimestamp() {
		return timestamp;
	}
	
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public long getTransactionNumber() {
		return transactionNumber;
	}

	public void setTransactionNumber(long transactionNumber) {
		this.transactionNumber = transactionNumber;
	}
}
