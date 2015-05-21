package rdtrc.interfaceManagement;

import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import rdtrc.logManager.LogManager;
import rdtrc.accessManagement.AccessManager;
import rdtrc.bufferManagement.BufferManagement;
import rdtrc.global.Constant;
import rdtrc.global.Logger;
import rdtrc.structures.LockEntity;
import rdtrc.structures.RowRecord;
import rdtrc.structures.Tuple;

public class AccessInterface {

	private Logger logger;
	private LogManager logManager;
	private static AccessInterface instance = null;
	private AccessManager accessManager;
	private BufferManagement bufferManager;

	private AccessInterface() {
		logger = Logger.getInstance();
		logManager = LogManager.getInstance();
		accessManager = AccessManager.getInstance();
		bufferManager = BufferManagement.getInstance();
	}

	public static AccessInterface getInstance() {
		if (instance == null) {
			instance = new AccessInterface();
		}
		return instance;
	}

	public void acknowledgeCompletion(long transactionTimestamp, boolean isCommit) {
		bufferManager.executeTransaction(transactionTimestamp, isCommit);
	}
	
	public boolean passOperationToDataManager(LockEntity lockEntity) {
		boolean success = true;

		String logEntry = "";

		String newLockEntityOperation = lockEntity.getOperation();

		if (newLockEntityOperation
				.equalsIgnoreCase(Constant.retrieveByAreaCodeCommand)) {
			logEntry = getRecordByAreaCode(lockEntity.getTableName(),
					lockEntity.getAreaCode());
		} else if (newLockEntityOperation
				.equalsIgnoreCase(Constant.retrieveByIdCommand)) {
			logEntry = getRecordToDisplayById(lockEntity.getTableName(),
					Integer.toString(lockEntity.getId()));
		} else if (newLockEntityOperation
				.equalsIgnoreCase(Constant.groupByAreaCodeCountCommand)) {
			logEntry = getCountByAreaCode(lockEntity.getTableName(),
					lockEntity.getAreaCode());
		} else if (newLockEntityOperation
				.equalsIgnoreCase(Constant.insertCommand)) {
			long logSequenceNumber = logManager.logEntry(lockEntity);
			try {
				logEntry = insertRecord(lockEntity.getTableName(),
						Integer.toString(lockEntity.getId()),
						lockEntity.getClientName(), lockEntity.getPhoneNumber(), logSequenceNumber);
			} catch (Exception e) {
				success = false;
			}
		} else if (newLockEntityOperation
				.equalsIgnoreCase(Constant.deleteCommand)) {
			logManager.logEntry(lockEntity);
		}

		System.out.println(logEntry);
		logger.WriteLog(logEntry);

		return success;
	}

	public String getRecordById(String tableName, String id) {
		String result = null;
		RowRecord queryResult = null;
		int hashValue = accessManager.getHashValue(id);
		List<Integer> offsets = accessManager.getOffsets(tableName, id);
		if (offsets != null) {
			queryResult = bufferManager.getRowRecord(tableName, hashValue,
					offsets, id);
			if (queryResult != null) {
				result = queryResult.toString();
			}
		}
		return result;
	}

	public String getRecordToDisplayById(String tableName, String id) {
		String result = getRecordById(tableName, id);

		if (result == null) {
			result = "No record found of Id " + id + ".";
		} else {
			result = "Record found for Id " + id + " in entity " + tableName
					+ ". " + result;
		}

		return result;
	}

	public String getCountByAreaCode(String tableName, String _areaCode) {
		String result = "";

		int areaCode = Integer.parseInt(_areaCode);

		int count = 0;

		HashMap<Tuple<Integer, Integer>, List<Integer>> offsetList = accessManager
				.getSecondaryOffsets(tableName, areaCode);

		if (offsetList != null) {
			for (Entry<Tuple<Integer, Integer>, List<Integer>> offset : offsetList
					.entrySet()) {
				List<Integer> recordList = offset.getValue();

				if (recordList != null) {
					count += recordList.size();
				}
			}
		}

		if (count == 0) {
			result = "Record not found for area code " + _areaCode
					+ " in entity " + tableName + ".";
		} else {
			result = "Count of record for area code " + _areaCode
					+ " in entity " + tableName + " is " + count + ".";
		}

		return result;
	}

	public String getRecordByAreaCode(String tableName, String _areaCode) {
		StringBuilder result = new StringBuilder("");

		int areaCode = Integer.parseInt(_areaCode);

		HashMap<Tuple<Integer, Integer>, List<Integer>> offsetList = accessManager
				.getSecondaryOffsets(tableName, areaCode);

		if (offsetList != null) {
			List<RowRecord> records = bufferManager.getRowRecords(tableName,
					offsetList);
			if (records != null) {
				result.append("Records for area code " + _areaCode);
				result.append(System.getProperty("line.separator"));
				for (RowRecord item : records) {
					result.append(item.toString());
					result.append(System.getProperty("line.separator"));
				}
			}
		}

		if (result.toString().isEmpty()) {
			result.append("No record found for area code " + _areaCode
					+ " in entity " + tableName + ".");
		}

		return result.toString();
	}

	public String insertRecord(String tableName, String id, String clientName,
			String phoneNumber, long lsn) throws Exception {
		String result = "";
		createTable(tableName);
		String queryResult = getRecordById(tableName, id);
		if (queryResult == null) {
			bufferManager.insertRowRecord(tableName, new RowRecord(id,
					clientName, phoneNumber), accessManager.getHashValue(id),
					accessManager.getOffsets(tableName, id), lsn, false);
			result = "Insert successful for record [Id: " + id
					+ ", Client Name: " + clientName + ", Phone Number: "
					+ phoneNumber + "].";
		} else {
			throw new Exception("Abort due to IC violation");
		}

		return result;
	}

	public String storeIndexes() {
		String result = "index stored";
		accessManager.storeIndexesInFile();
		return result;
	}

	public String storeTables() {
		String result = "tables stored";
		bufferManager.flushTablesToFile();
		return result;
	}

	public String storeSecondayIndex() {
		String result = "index stored";
		accessManager.storeSecondayIndexInFile();
		return result;
	}

	public String createTable(String tableName) {
		String result = "table created";
		accessManager.createTable(tableName);
		accessManager.createSecondaryIndex(tableName);
		return result;
	}
}
