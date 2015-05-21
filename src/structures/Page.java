package rdtrc.structures;

public abstract class Page implements Comparable<Page> {

	String tableName;
	int hashValue;
	int offset;
	int usageCounter;
	boolean dirtyBit;
	
	public Page() {
		// TODO
	}
	
	public Page(String tableName, int hashValue, int offset, int usageCounter,
			boolean dirtyBit) {
		super();
		this.tableName = tableName;
		this.hashValue = hashValue;
		this.offset = offset;
		this.usageCounter = usageCounter;
		this.dirtyBit = dirtyBit;
	}



	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
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

	public int getUsageCounter() {
		return usageCounter;
	}

	public void setUsageCounter(int usageCounter) {
		this.usageCounter = usageCounter;
	}

	public boolean isDirty() {
		return dirtyBit;
	}

	public void setDirtyBit(boolean dirtyBit) {
		this.dirtyBit = dirtyBit;
	}

	@Override
	public int compareTo(Page p) {
		int result;

		if(this.usageCounter == p.usageCounter)
			return 0;
		else if(this.usageCounter > p.usageCounter)
			result = 1;
		else
			result = -1;

		return result;		
	}

}
