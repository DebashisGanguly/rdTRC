package rdtrc.structures;

public class ColumnRecord {

	String value;
	
	public ColumnRecord() {
		
	}
	
	public ColumnRecord(String value) {
		super();
		this.value = value;
	}
	
	public ColumnRecord(ColumnRecord record) {
		super();
		this.value = record.value;
	}

	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
}
