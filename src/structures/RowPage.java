package rdtrc.structures;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import rdtrc.global.Constant;

public class RowPage extends Page {
	
	int index;
	int numberOfRows;
	List<RowRecord> records;	

	public RowPage() {
		super();
		index = -1;
		numberOfRows = Constant.pageSize/(Constant.clientNameSize + Constant.idSize + Constant.phoneNumberSize);
		records = new ArrayList<RowRecord>();
	}

	public RowPage(String tableName, int hashValue, int offset,
			int usageCounter, boolean dirtyBit, int index) {
		super(tableName, hashValue, offset, usageCounter, dirtyBit);
		if (index == 7)
			numberOfRows = Constant.pageSize/(Constant.clientNameSize + Constant.idSize + Constant.phoneNumberSize) - 2;
		else
			numberOfRows = Constant.pageSize/(Constant.clientNameSize + Constant.idSize + Constant.phoneNumberSize);
		records = new ArrayList<RowRecord>();
		// TODO Auto-generated constructor stub
	}
	
	public RowPage(RowPage page) {
		super(page.tableName, page.hashValue, page.offset,
				page.usageCounter, page.dirtyBit);
		numberOfRows = Constant.pageSize/(Constant.clientNameSize + Constant.idSize + Constant.phoneNumberSize);
		records = new ArrayList<RowRecord>(page.records);
		this.index = page.index;
		// TODO Auto-generated constructor stub
	}
	
	public int getNumberOfRows() {
		return records.size();
	}
	
	public int getIndex() {
		return index;
	}
	
	public void setIndex(int index) {
		this.index = index;
	}

	public RowRecord getRecord(String id) {
		RowRecord result = null;
		
		Iterator<RowRecord> iteratorRowRecord = records.iterator();
		while(iteratorRowRecord.hasNext()) {
			result = iteratorRowRecord.next();
			if (result != null && result.getId().equals(id)) {
				break;
			}
			result = null;
		}
		
		return result;
	}
	
	public void deleteRecord(String id) {
		if(records != null && !records.isEmpty())
		{
			for(int i = 0; i < records.size(); i++) {
				if(records.get(i).getId().equals(id)) {
					records.remove(i);
					break;
				}
			}
		}
		else
			;//exception
	}
	
	public void insertRecord(RowRecord record) {
		if(!isFull())
			records.add(new RowRecord(record));
		else
			;//exception
	}
	
	public boolean isFull() {
		return numberOfRows == records.size();
	}

	@Override
	public String toString() {
		return "RowPage [numberOfRows=" + numberOfRows + ", records="
				+ records + ", usageCounter=" + usageCounter + "]";
	}
}
