package rdtrc.structures;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import rdtrc.global.Constant;

public class ColumnPage extends Page {
	
	private int index;
	private int cpacity;
	private String columnName;
	private List<ColumnRecord> records;
	
	public ColumnPage() {
		super();
	}


	public ColumnPage(String columnName) {
		super();
		this.columnName = columnName;
		cpacity = getCpacity(columnName);	
		records = new ArrayList<ColumnRecord>();
	}

	public ColumnPage(String columnName, String tableName, int hashValue, int offset,
			int usageCounter, boolean dirtyBit, int index) {
		super(tableName, hashValue, offset, usageCounter, dirtyBit);
		this.columnName = columnName;
		this.index = index;
		cpacity = getCpacity(columnName);
		records = new ArrayList<ColumnRecord>();
	}
	
	public ColumnPage(ColumnPage page) {
		super(page.tableName, page.hashValue, page.offset, page.usageCounter, 
				page.dirtyBit);
		this.columnName = page.columnName;
		this.index = page.index;
		cpacity = page.cpacity;
		records = new ArrayList<ColumnRecord>(page.records);
	}
	
	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public List<ColumnRecord> getRecords() {
		return records;
	}
	
	public void insertValue(String value) {
		if(!isFull())
			records.add(new ColumnRecord(value));
		else
			;//exception
	}
	
	public int getIndex() {
		return index;
	}
	
	public void setIndex(int i) {
		index = i;
	}

	public ColumnRecord doesRecordExist(String value) {
		ColumnRecord result = null;
		boolean found = false;
		
		Iterator<ColumnRecord> iterator = records.iterator();
		while(iterator.hasNext() && !found) {
			result = iterator.next();
			if (result != null && result.getValue().equals(value)) {
				found = true;
			}
		}
		
		return new ColumnRecord(result);
	}
	
	public void deleteRecord(String value) {
		if(records != null && !records.isEmpty())
		{
			for(int i = 0; i < records.size(); i++) {
				if(records.get(i).getValue().equals(value)) {
					records.remove(i);
					break;
				}
			}
		}
		else
			;//exception
	}

	
	
	public boolean isFull() {
		return cpacity == records.size();
	}

	@Override public String toString() {
		StringBuilder sb = new StringBuilder();

		for (ColumnRecord item : records) {
			if(sb.length() > 0){
		        sb.append(',');
		    }
		    sb.append(item.getValue());
		}

		return sb.toString();
	}
	
	public int getNumberOfValues() {
		return records.size();
	}


	private int getCpacity(String columnName) {
		if(columnName.equalsIgnoreCase(Constant.id))		
			return Constant.pageSize/Constant.idSize - 2;
		else if(columnName.equalsIgnoreCase(Constant.clientName)) {
			if (index == 3)
				return Constant.pageSize/Constant.clientNameSize - 2 ;
			else
				return Constant.pageSize/Constant.clientNameSize;
		}
		else if(columnName.equalsIgnoreCase(Constant.phoneNumber))			
			return Constant.pageSize/Constant.phoneNumberSize;
		else
			return -1;
	}

}
