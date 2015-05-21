package rdtrc.accessManagement;

import java.util.ArrayList;
import java.util.HashMap;

import rdtrc.fileManagement.FileManagement;

import java.util.List;

import rdtrc.structures.SecondaryIndex;
import rdtrc.structures.TableIndex;
import rdtrc.structures.Tuple;

public class AccessManager {

	private static AccessManager instance = null;
	private HashMap<String, TableIndex> indexes;
	private HashMap<String, SecondaryIndex> secondaryIndices;
	private FileManagement fileManager;

	private AccessManager() {
		fileManager = FileManagement.getInstance();
		indexes = fileManager.readIndex();
		secondaryIndices = fileManager.readSecondaryIndex();
	}

	public static AccessManager getInstance() {
		if (instance == null) {
			instance = new AccessManager();
		}
		return instance;
	}

	public List<Integer> getOffsets(String tableName, String id) {
		List<Integer> result = null;
		int bucketValue = getHashValue(id);
		TableIndex index = indexes.get(tableName);
		if (index != null) {
			if (index.getOffsetList(bucketValue) != null)
				result = new ArrayList<Integer>(
						index.getOffsetList(bucketValue));
			else
				;// exception
		} else {
			// exception
		}
		return result;
	}

	public HashMap<Tuple<Integer, Integer>, List<Integer>> getSecondaryOffsets(
			String tableName, int areaCode) {
		return secondaryIndices.get(tableName) == null ? null
				: (secondaryIndices.get(tableName).get(areaCode) == null ? null
						: new HashMap<Tuple<Integer, Integer>, List<Integer>>(
								secondaryIndices.get(tableName).get(areaCode)));
	}

	public int addOffset(String tableName, String id) {
		int result = -1;
		int bucketValue = getHashValue(id);
		createTable(tableName);
		TableIndex index = indexes.get(tableName);
		result = index.addPageOffset(bucketValue);
		return result;
	}

	public void addSecondaryIndex(String tableName, int areaCode,
			int hashValue, int offset, int newRecordId) {
		SecondaryIndex sIndex = secondaryIndices.get(tableName);

		if (sIndex == null) {
			sIndex = new SecondaryIndex(tableName);
		}

		HashMap<Tuple<Integer, Integer>, List<Integer>> offsetList = sIndex
				.get(areaCode);

		if (offsetList == null) {
			offsetList = new HashMap<Tuple<Integer, Integer>, List<Integer>>();
		}

		if (offsetList.get(new Tuple<Integer, Integer>(hashValue, offset)) == null) {
			offsetList.put(new Tuple<Integer, Integer>(hashValue, offset),
					new ArrayList<Integer>());
		}
		offsetList.get(new Tuple<Integer, Integer>(hashValue, offset)).add(
				newRecordId);

		sIndex.put(areaCode, offsetList);
	}

	public void deleteSecondaryIndex(String tableName, int areaCode,
			int hashValue, int offset, int newRecordId) {
		SecondaryIndex sIndex = secondaryIndices.get(tableName);

		if (sIndex != null) {
			HashMap<Tuple<Integer, Integer>, List<Integer>> offsetList = sIndex
					.get(areaCode);

			if (offsetList != null) {

				if (offsetList.get(new Tuple<Integer, Integer>(hashValue,
						offset)) != null && offsetList.get(new Tuple<Integer, Integer>(hashValue,
								offset)).size() > newRecordId) {
					offsetList.get(
							new Tuple<Integer, Integer>(hashValue, offset))
							.remove(newRecordId);
					sIndex.put(areaCode, offsetList);
				}
			}
		}
	}

	public void createTable(String tableName) {
		TableIndex index = indexes.get(tableName);
		if (index == null) {
			index = new TableIndex(tableName);
			indexes.put(tableName, index);
		}
	}

	public void createSecondaryIndex(String tableName) {
		SecondaryIndex sIndex = secondaryIndices.get(tableName);
		if (sIndex == null) {
			sIndex = new SecondaryIndex(tableName);
			secondaryIndices.put(tableName, sIndex);
		}
	}

	public void storeIndexesInFile() {
		fileManager.writeIndex(indexes);
	}

	public void storeSecondayIndexInFile() {
		fileManager.writeSecondaryIndex(secondaryIndices);
	}

	public void deleteTable(String tableName) {
		TableIndex index = indexes.get(tableName);
		if (index != null) {
			indexes.remove(tableName);
			fileManager.deleteTable(tableName);
		}

		SecondaryIndex sIndex = secondaryIndices.get(tableName);
		if (sIndex != null) {
			secondaryIndices.remove(tableName);
		}
	}

	public int getHashValue(String id) {
		int result = -1;
		result = Integer.parseInt(id);
		result = result % 16;
		return result;
	}
}
