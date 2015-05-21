package rdtrc.structures;

public class LogEntry {
	private LockEntity lockEntity;
	private int hashValue;
	private int offset;
	private int recordPosition;
	
	public LogEntry(LockEntity lockEntity, int hashValue, int offset,
			int recordPosition) {
		super();
		this.lockEntity = lockEntity;
		this.hashValue = hashValue;
		this.offset = offset;
		this.recordPosition = recordPosition;
	}

	public LockEntity getLockEntity() {
		return lockEntity;
	}

	public void setLockEntity(LockEntity lockEntity) {
		this.lockEntity = lockEntity;
	}

	public int getHashValue() {
		return hashValue;
	}

	public void setHashValue(int hashValue) {
		this.hashValue = hashValue;
	}

	public int getOffset() {
		return offset;
	}

	public void setOffset(int offset) {
		this.offset = offset;
	}

	public int getRecordPosition() {
		return recordPosition;
	}

	public void setRecordPosition(int recordPosition) {
		this.recordPosition = recordPosition;
	}
}
