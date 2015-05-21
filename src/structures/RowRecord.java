package rdtrc.structures;

public class RowRecord {

	String id;
	String clientName;
	String phoneNumber;

	public RowRecord() {
		
	}

	public RowRecord(String id, String clientName, String phoneNumber) {
		super();
		this.id = id;
		this.clientName = clientName;
		this.phoneNumber = phoneNumber;
	}
	
	public RowRecord(RowRecord record) {
		super();
		this.id = record.id;
		this.clientName = record.clientName;
		this.phoneNumber = record.phoneNumber;
	}

	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getClientName() {
		return clientName;
	}
	public void setClientName(String clientName) {
		this.clientName = clientName;
	}
	public String getPhoneNumber() {
		return phoneNumber;
	}
	public void setPhoneNumber(String phoneNumber) {
		this.phoneNumber = phoneNumber;
	}

	@Override
	public String toString() {
		return "[Id: " + id + ", Client Name: " + clientName 
				+ ", Phone Number: " + phoneNumber + "]";
	}
}
