package rdtrc.structures;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

public class TableIndex {

	String tableName;
	private HashMap<Integer, List<Integer>> offsets;

	public TableIndex(String _tableName) {
		tableName = _tableName;
		offsets = new HashMap<Integer, List<Integer>>();
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	
	public HashMap<Integer, List<Integer>> getOffsets() {
		return offsets;
	}

	public void setOffsets(HashMap<Integer, List<Integer>> offsets) {
		this.offsets = offsets;
	}

	public List<Integer> getOffsetList(int hashValue) {
		List<Integer> result = null;
		result = offsets.get(hashValue);
		return result;
	}

	public int addPageOffset(int hashValue) {
		int newOffset = getNewOffset();
		List<Integer> offsetList = offsets.get(hashValue);
		if(offsetList != null) {
			offsetList.add(newOffset);
		}
		else {
			offsetList = new LinkedList<Integer>();
			offsetList.add(newOffset);
			offsets.put(hashValue, offsetList);
		}
		return newOffset;
	}

	private int getNewOffset() {
		int numberOfNewPage = 0;
		for( Entry <Integer, List<Integer>> entry : offsets.entrySet() ) {
			List<Integer> list = entry.getValue();
			if(list != null)
				numberOfNewPage += list.size();

		}
		return numberOfNewPage;
	}

	@Override public String toString() {
		StringBuilder sb = new StringBuilder();

		for (Entry<Integer, List<Integer>> hashBucket : offsets.entrySet()) {
			sb.append(hashBucket.getKey().toString());
			sb.append("|");

			for(Integer offset : hashBucket.getValue()) {
				sb.append(offset.toString());
				sb.append(",");
			}

			sb.append(System.getProperty("line.separator"));
		}

		return sb.toString();
	}
}
