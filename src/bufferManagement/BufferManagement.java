package rdtrc.bufferManagement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import rdtrc.accessManagement.AccessManager;
import rdtrc.fileManagement.FileManagement;
import rdtrc.global.Constant;
import rdtrc.global.Logger;
import rdtrc.logManager.LogManager;
import rdtrc.structures.ColumnPage;
import rdtrc.structures.ColumnRecord;
import rdtrc.structures.Page;
import rdtrc.structures.LogEntry;
import rdtrc.structures.RowPage;
import rdtrc.structures.RowRecord;
import rdtrc.structures.Tuple;

public class BufferManagement {

	private static BufferManagement instance = null;
	private AccessManager accessManagerReference;
	private FileManagement fileManager;
	private Logger logger;

	private LogManager logManager;

	private int bufferSize;

	private LinkedList<RowPage> rowPages;
	private LinkedList<ColumnPage> columnPages;

	private ColumnPage[] holdInTemp;

	private BufferManagement() {
		fileManager = FileManagement.getInstance();
		fileManager.populateConfigurationItems();
		rowPages = new LinkedList<RowPage>();
		columnPages = new LinkedList<ColumnPage>();
		accessManagerReference = AccessManager.getInstance();
		logger = Logger.getInstance();
		bufferSize = Constant.bufferSize;
		holdInTemp = new ColumnPage[8];
		logManager = LogManager.getInstance();
	}

	public static BufferManagement getInstance() {
		if (instance == null) {
			instance = new BufferManagement();
		}
		return instance;
	}

	//TODO -- test
	public boolean executeTransaction(long timestamp, boolean isCommit)
	{
		boolean result = true;
		LinkedList<Long> lsnList = logManager.getLSNPerTransaction(timestamp);
		List<String> deletedTables = new ArrayList<String>();
		List<LogEntry> toCommit = new LinkedList<LogEntry>();

		if(lsnList != null)
		{
			if(isCommit)
			{
				for(int i = 0; i < lsnList.size(); i++)
				{
					LogEntry entry = logManager.getLogEntry(lsnList.get(i));
					if (entry.getLockEntity().getOperation().equals(Constant.insertCommand))
					{						
						if(deletedTables.contains(entry.getLockEntity().getTableName()))
							toCommit.add(entry);
					}
					else if(entry.getLockEntity().getOperation().equals(Constant.deleteCommand))
					{
						String logEntry = "Committed Delete: " + entry.getLockEntity().getTableName();

						System.out.println(logEntry);
						logger.WriteLog(logEntry);

						actualDeleteTableFromBuffer(entry.getLockEntity().getTableName());
						accessManagerReference.deleteTable(entry.getLockEntity().getTableName());
						deletedTables.add(entry.getLockEntity().getTableName());
					}
				}
				for(int i = 0; i < lsnList.size(); i++)
				{

					LogEntry entry = logManager.getLogEntry(lsnList.get(i));
					if (entry.getLockEntity().getOperation().equals(Constant.insertCommand))
					{
						if(!deletedTables.contains(entry.getLockEntity().getTableName()))
						{
							String logEntry = "Committed Insert: " + entry.getLockEntity().getId() + ", "
									+ entry.getLockEntity().getClientName() + ", " + entry.getLockEntity().getPhoneNumber();

							System.out.println(logEntry);
							logger.WriteLog(logEntry);
						}
					}
				}
				for(int i = 0; i < toCommit.size(); i++)
				{
					LogEntry entry = toCommit.get(i);
					String logEntry = "Committed Insert: " + entry.getLockEntity().getId() + ", "
							+ entry.getLockEntity().getClientName() + ", " + entry.getLockEntity().getPhoneNumber();

					System.out.println(logEntry);
					logger.WriteLog(logEntry);

					insertRowRecord(entry.getLockEntity().getTableName(),
							new RowRecord(String.valueOf(entry.getLockEntity().getId()), entry.getLockEntity().getClientName(), entry.getLockEntity().getPhoneNumber()),
							entry.getHashValue(), 
							accessManagerReference.getOffsets(entry.getLockEntity().getTableName(),
									String.valueOf(entry.getLockEntity().getId())), -1, true);
				}
			}
			else // it is abort
			{
				for(int i = 0; i < lsnList.size(); i++)
				{
					LogEntry entry = logManager.getLogEntry(lsnList.get(i));
					if (entry.getLockEntity().getOperation().equals(Constant.insertCommand))
					{
						String logEntry = "Aborted Insert: " + entry.getLockEntity().getId() + ", "
								+ entry.getLockEntity().getClientName() + ", " + entry.getLockEntity().getPhoneNumber();

						System.out.println(logEntry);
						logger.WriteLog(logEntry);


						RowPage rPage = new RowPage();
						ColumnPage cPage = new ColumnPage();
						rPage.setHashValue(entry.getHashValue());
						rPage.setOffset(entry.getOffset());
						rPage.setTableName(entry.getLockEntity().getTableName());
						cPage.setHashValue(entry.getHashValue());
						cPage.setOffset(entry.getOffset());
						cPage.setTableName(entry.getLockEntity().getTableName());
						List<Integer> list = new ArrayList<Integer>();
						list.add(entry.getOffset());
						getRowRecord(entry.getLockEntity().getTableName(), entry.getHashValue(), list, String.valueOf(entry.getLockEntity().getId()));
						removeRecordFromPages(String.valueOf(entry.getLockEntity().getId()), getColumnPagesByOffset(cPage), 
								getRowPagesByOffset(rPage));
						
						accessManagerReference.deleteSecondaryIndex(entry.getLockEntity().getTableName(), 
								Integer.parseInt(entry.getLockEntity().getPhoneNumber().substring(0, 3)), entry.getHashValue(),
								entry.getOffset(), entry.getRecordPosition());
					}
					else if (entry.getLockEntity().getOperation().equals(Constant.deleteCommand))
					{
						String logEntry = "Aborted Delete: " + entry.getLockEntity().getTableName();

						System.out.println(logEntry);
						logger.WriteLog(logEntry);

					}
				}
			}
		}
		else
			result = false;

		return result;
	}

	public List<RowRecord> getRowRecords (String tableName,
			HashMap<Tuple<Integer, Integer>, List<Integer>> offsetList) {
		List<RowRecord> result = new LinkedList<RowRecord>();
		ColumnPage page = null;
		ColumnPage[] subsetColumns = null;
		RowPage[] subsetRows = null;
		boolean flag = false;
		Iterator<ColumnPage> iteratorColumnPages = columnPages.iterator();

		while (iteratorColumnPages.hasNext() && !flag) {
			page = iteratorColumnPages.next();
			if (page != null) {
				if (page.getTableName().equals(tableName)
						&& offsetList.containsKey(new Tuple<Integer, Integer>(
								page.getHashValue(), page.getOffset()))) {
					List<Integer> recordLocations = offsetList
							.remove(new Tuple<Integer, Integer>(page
									.getHashValue(), page.getOffset()));
					subsetColumns = getColumnPagesByOffset(page);
					if (subsetColumns != null) {
						result.addAll(fetchRecords(subsetColumns,
								recordLocations));
						RowPage toIncrement = new RowPage(page.getTableName(), page.getHashValue(), page.getOffset(), 0, true, 0);
						incrementRowPageCounter(getRowPagesByOffset(toIncrement));
						incrementColumnPageCounter(toIncrement);
						if (offsetList.isEmpty())
							flag = true;
					}
				}
			}
		}

		Iterator<Entry<Tuple<Integer, Integer>, List<Integer>>> iteratorOffset = offsetList
				.entrySet().iterator();
		while (iteratorOffset.hasNext()) {
			Map.Entry<Tuple<Integer, Integer>, List<Integer>> entry = (Entry<Tuple<Integer, Integer>, List<Integer>>) iteratorOffset
					.next();
			int offsetNumber = (Integer) entry.getKey().getRight();
			int hashValue = (Integer) entry.getKey().getLeft();
			List<Integer> recordLocations = (List<Integer>) entry.getValue();
			subsetRows = readRowPageFromDesk(tableName, hashValue, offsetNumber);
			incrementRowPageCounter(subsetRows);
			addPageToRowBuffer(subsetRows);
			result.addAll(fetchRecords(holdInTemp, recordLocations));
			incrementColumnPageCounter(subsetRows[0]);
			iteratorOffset.remove();
		}

		return result;
	}

	public RowRecord getRowRecord(String tableName, int hashValue,
			List<Integer> offsets, String id) {
		RowRecord result = null;
		RowPage page = null;
		RowPage[] subset = null;
		boolean flag = false;
		Iterator<RowPage> iteratorRowPages = rowPages.iterator();

		while (iteratorRowPages.hasNext() && !flag) {
			page = iteratorRowPages.next();
			if (page != null) {
				if (page.getTableName().equals(tableName)
						&& page.getHashValue() == hashValue) {
					if (offsets.contains(page.getOffset())) {
						subset = getRowPagesByOffset(page);
						if (subset != null) {
							result = lookForTheRecordInPages(subset, id);
							if (result != null)
								flag = true;
							offsets.remove(offsets.indexOf(page.getOffset()));
							if (offsets.isEmpty())
								flag = true;
						}
					}
				}
			}
		}

		if (result == null) {
			flag = false;
			while (!offsets.isEmpty() && !flag) {
				subset = readRowPageFromDesk(tableName, hashValue, offsets
						.remove(0).intValue());
				addPageToRowBuffer(subset);
				result = lookForTheRecordInPages(subset, id);
				if (result != null)
					flag = true;
			}
		}

		return result;
	}

	//TODO -- test
	public void insertRowRecord(String tableName, RowRecord record,
			int hashValue, List<Integer> offsets, long lsn, boolean executionRun) {
		RowPage[] subset = new RowPage[8];
		RowPage page = null;
		boolean pageFound = false;
		int maxOffset = -1;
		int indexOfFreePage = -1;

		if (offsets != null) {
			maxOffset = Collections.max(offsets);
		} else {
			maxOffset = accessManagerReference.addOffset(tableName,
					record.getId());
			accessManagerReference.createSecondaryIndex(tableName);
		}

		Iterator<RowPage> iteratorRowPages = rowPages.iterator();
		while (iteratorRowPages.hasNext() && !pageFound) {
			page = iteratorRowPages.next();
			if (page != null) {
				if (page.getTableName().equals(tableName)
						&& page.getHashValue() == hashValue
						&& page.getOffset() == maxOffset) {
					pageFound = true;
				}
			}
		}

		if (!pageFound) {
			subset = readRowPageFromDesk(tableName, hashValue, maxOffset);
			addPageToRowBuffer(subset);
		} else {
			subset = getRowPagesByOffset(page);
		}

		indexOfFreePage = getIndexOfPageWithSpace(subset);

		int newRecordId = 0;
		int offset = maxOffset;

		if (indexOfFreePage != -1) {
			subset[indexOfFreePage].insertRecord(record);
			setDirtyBitForPages(subset);
			incrementRowPageCounter(subset);
			newRecordId = indexOfFreePage * 16 + subset[indexOfFreePage].getNumberOfRows() - 1;
			insertRowRecordIntoColumnPage(record, subset[indexOfFreePage],
					newRecordId);

			//update the log
			if(!executionRun)
			{
				LogEntry entry = logManager.getLogEntry(lsn);
				entry.setRecordPosition(newRecordId);
				entry.setOffset(offset);
				entry.setHashValue(hashValue);
				logManager.updateScratchPaper(lsn, entry);
			}
		} else {
			RowPage newPage = new RowPage(
					tableName,
					hashValue,
					accessManagerReference.addOffset(tableName, record.getId()),
					1, true, 0);
			subset = addNewRowPageWithNewRecord(newPage, record);
			insertRowRecordIntoNewColumnPage(subset, record);
			addPageToRowBuffer(subset);
			offset = newPage.getOffset();

			//update the log
			if(!executionRun)
			{
				LogEntry entry = logManager.getLogEntry(lsn);
				entry.setRecordPosition(newRecordId);
				entry.setOffset(offset);
				entry.setHashValue(hashValue);
				logManager.updateScratchPaper(lsn, entry);
			}
		}

		accessManagerReference.addSecondaryIndex(tableName,
				Integer.parseInt(record.getPhoneNumber().substring(0, 3)),
				hashValue, offset, newRecordId);

	}

	public void flushTablesToFile() {
		for (int i = 0; i < rowPages.size(); i++) {
			if (rowPages.get(i).isDirty()) {
				writePagesToDesk(rowPages.get(i), true, false);
			}
		}
	}

	// ///////////////////////////////////////////

	// -- CHECK BUFFER SIZE
	private boolean isPagesBufferFull() {
		boolean result = false;

		if (rowPages.size() == bufferSize / 2
				&& columnPages.size() == bufferSize / 2) {
			result = true;
		}

		return result;
	}

	// -- CONSTRUCT BRAND NEW ROW PAGE FOR INSERTION
	private RowPage[] addNewRowPageWithNewRecord(RowPage page, RowRecord record) {
		RowPage[] newSubset = new RowPage[8];

		for (int i = 0; i < 8; i++) {
			newSubset[i] = new RowPage(page.getTableName(),
					page.getHashValue(), page.getOffset(), 1, true, i);
		}

		newSubset[0].insertRecord(record);

		return newSubset;
	}

	// -- RETURN THE INDEX OF THE PAGE IN THE BUFFER
	// -- THAT HAS SPACE FOR INSERTING A NEW ROW RECORD
	private int getIndexOfPageWithSpace(RowPage[] subset) {
		int result = -1;

		for (int i = 0; i < subset.length; i++) {
			if (!subset[i].isFull()) {
				result = i;
				break;
			}
		}

		return result;
	}

	// -- ADDING PAGES TO THE BUFFERS
	private void addPageToRowBuffer(RowPage[] toInsert) {
		Collections.sort(rowPages);
		Collections.sort(columnPages);
		RowPage[] toEvict = null;
		boolean dirty = false;

		// evict if full
		if (isPagesBufferFull()) {
			// check if they are dirty?
			// write the corresponding column ones to the desk
			toEvict = toEvictRowPages();

			String logEntry = "SWAP OUT T-" + toEvict[0].getTableName() + " P-"
					+ toEvict[0].getOffset() + " B-"
					+ toEvict[0].getHashValue();

			System.out.println(logEntry);
			logger.WriteLog(logEntry);

			dirty = isPagesDirty(toEvict);
			if (dirty) {
				writePagesToDesk(toEvict[0], true, true);
			} else {
				writePagesToDesk(toEvict[0], false, true);
			}
		}

		// add the new pages
		String logEntry = "";
		if (toInsert[0].getNumberOfRows() == 0) {
			logEntry = "CREATE T-" + toInsert[0].getTableName() + " P-"
					+ toInsert[0].getOffset() + " B-"
					+ toInsert[0].getHashValue();

			System.out.println(logEntry);
			logger.WriteLog(logEntry);
		}

		logEntry = "SWAP IN T-" + toInsert[0].getTableName() + " P-"
				+ toInsert[0].getOffset() + " B-" + toInsert[0].getHashValue();

		System.out.println(logEntry);
		logger.WriteLog(logEntry);

		for (int i = 0; i < 8; i++) {
			rowPages.add(toInsert[i]);
			columnPages.add(holdInTemp[i]);
		}

		Collections.sort(rowPages);
		Collections.sort(columnPages);
	}

	// -- CHECK DIRTINESS OF A PAGE (8 pages)
	private boolean isPagesDirty(Page[] toEvict) {
		boolean result = false;

		for (int i = 0; i < 8; i++) {
			if (toEvict[i].isDirty())
				result = true;
		}

		return result;
	}

	// -- SET DIRTINESS OF A PAGE (8 PAGES)
	private void setDirtyBitForPages(Page[] pages) {
		for (int i = 0; i < 8; i++) {
			pages[i].setDirtyBit(true);
		}
	}

	// -- RETURN THE [Column | ROW] PAGE (8 PAGES) TO BE EVICTED
	private RowPage[] toEvictRowPages() {
		RowPage[] result = null;
		if (!rowPages.isEmpty()) {
			result = new RowPage[8];
			result[0] = rowPages.removeFirst();
			RowPage toEvict = null;
			int i = 1;

			Iterator<RowPage> iterator = rowPages.iterator();
			while (iterator.hasNext()) {
				toEvict = iterator.next();
				if (toEvict != null
						&& toEvict.getHashValue() == result[0].getHashValue()
						&& toEvict.getTableName() == result[0].getTableName()
						&& toEvict.getOffset() == result[0].getOffset()) {
					result[i++] = toEvict;
					iterator.remove();
				}
			}
		}
		return result;
	}

	// -- GET THE ROW RECORD IN PAGE (8 PAGES)
	private RowRecord lookForTheRecordInPages(RowPage[] set, String id) {
		RowRecord result = null;

		for (int i = 0; i < 8; i++) {
			result = set[i].getRecord(id);
			if (result != null) {
				break;
			}
		}
		if (result != null) {
			incrementRowPageCounter(set);
			incrementColumnPageCounter(set[0]);
		}

		return result;
	}

	// -- FETCHING THE ROW RECORDS FROM
	// -- THE GIVEN PAGE BY LOCATION
	private List<RowRecord> fetchRecords(ColumnPage[] pages,
			List<Integer> recordLocations) {
		List<RowRecord> result = null;
		String id = "";
		String name = "";
		String phone = "";

		if (pages != null && recordLocations != null) {
			result = new LinkedList<RowRecord>();
			for (int i = 0; i < recordLocations.size(); i++) {
				int location = recordLocations.get(i);

				id = pages[0].getRecords().get(location).getValue();

				if (location < 32)
					name = pages[1].getRecords().get(location).getValue();
				else if (location < 64)
					name = pages[2].getRecords().get(location - 32).getValue();
				else if (location < 96)
					name = pages[3].getRecords().get(location - 64).getValue();
				else if (location <= 126)
					name = pages[4].getRecords().get(location - 96).getValue();

				if (location < 42)
					phone = pages[5].getRecords().get(location).getValue();
				else if (location < 84)
					phone = pages[6].getRecords().get(location - 42).getValue();
				else if (location <= 126)
					phone = pages[7].getRecords().get(location - 84).getValue();

				result.add(new RowRecord(id, name, phone));
			}
		}
		return result;
	}

	// -- GET THE PAGE FROM THE BUFFER BY OFFSET
	private RowPage[] getRowPagesByOffset(RowPage page) {
		RowPage[] result = new RowPage[8];
		int j = 0;

		for (int i = 0; i < rowPages.size(); i++) {
			if (rowPages.get(i).getTableName().equals(page.getTableName())
					&& rowPages.get(i).getHashValue() == page.getHashValue()
					&& rowPages.get(i).getOffset() == page.getOffset()) {
				result[j++] = rowPages.get(i);
			}
		}

		return result;
	}

	private ColumnPage[] getColumnPagesByOffset(ColumnPage page) {
		ColumnPage[] result = new ColumnPage[8];
		int idOffset = page.getOffset();

		for (int i = 0; i < columnPages.size(); i++) {
			if (columnPages.get(i).getTableName().equals(page.getTableName())
					&& columnPages.get(i).getHashValue() == page.getHashValue()) {
				if (columnPages.get(i).getOffset() == idOffset
						&& columnPages.get(i).getColumnName()
						.equals(Constant.id)) {
					result[0] = columnPages.get(i);
				} else if (columnPages.get(i).getOffset() == idOffset * 4
						&& columnPages.get(i).getColumnName()
						.equals(Constant.clientName)) {
					result[1] = columnPages.get(i);
				} else if (columnPages.get(i).getOffset() == idOffset * 4 + 1
						&& columnPages.get(i).getColumnName()
						.equals(Constant.clientName)) {
					result[2] = columnPages.get(i);
				} else if (columnPages.get(i).getOffset() == idOffset * 4 + 2
						&& columnPages.get(i).getColumnName()
						.equals(Constant.clientName)) {
					result[3] = columnPages.get(i);
				} else if (columnPages.get(i).getOffset() == idOffset * 4 + 3
						&& columnPages.get(i).getColumnName()
						.equals(Constant.clientName)) {
					result[4] = columnPages.get(i);
				} else if (columnPages.get(i).getOffset() == idOffset * 3
						&& columnPages.get(i).getColumnName()
						.equals(Constant.phoneNumber)) {
					result[5] = columnPages.get(i);
				} else if (columnPages.get(i).getOffset() == idOffset * 3 + 1
						&& columnPages.get(i).getColumnName()
						.equals(Constant.phoneNumber)) {
					result[6] = columnPages.get(i);
				} else if (columnPages.get(i).getOffset() == idOffset * 3 + 2
						&& columnPages.get(i).getColumnName()
						.equals(Constant.phoneNumber)) {
					result[7] = columnPages.get(i);
				}
			}
		}
		return result;
	}

	// -- READ ROW PAGES FROM DESK THROUGH THE COLUMN LEVEL

	private RowPage[] readRowPageFromDesk(String tableName, int hashValue,
			int offset) {
		RowPage[] result = null;
		holdInTemp = new ColumnPage[8];

		holdInTemp[0] = fileManager.fetchPage(tableName, Constant.id,
				hashValue, offset);
		for (int i = 0; i < 4; i++) {
			holdInTemp[i + 1] = fileManager.fetchPage(tableName,
					Constant.clientName, hashValue, offset * 4 + i);
		}
		for (int i = 0; i < 3; i++) {
			holdInTemp[i + 5] = fileManager.fetchPage(tableName,
					Constant.phoneNumber, hashValue, offset * 3 + i);
		}

		result = constructRowPageFromColumnPages();

		return result;
	}

	// -- PART OF THE PREVIOUS METHOD: CONSTRUCT ROW PAGES FROM COLUMN PAGES
	private RowPage[] constructRowPageFromColumnPages() {
		RowRecord toInsert = null;
		boolean flag = true;

		RowPage[] result = new RowPage[8];
		for (int i = 0; i < result.length; i++) {
			result[i] = new RowPage(holdInTemp[0].getTableName(),
					holdInTemp[0].getHashValue(), holdInTemp[0].getOffset(), 0,
					false, i);
		}

		List<ColumnRecord> ids = holdInTemp[0].getRecords();
		List<ColumnRecord> names1 = holdInTemp[1].getRecords();
		List<ColumnRecord> names2 = holdInTemp[2].getRecords();
		List<ColumnRecord> names3 = holdInTemp[3].getRecords();
		List<ColumnRecord> names4 = holdInTemp[4].getRecords();
		List<ColumnRecord> phone1 = holdInTemp[5].getRecords();
		List<ColumnRecord> phone2 = holdInTemp[6].getRecords();
		List<ColumnRecord> phone3 = holdInTemp[7].getRecords();

		for (int i = 0; i < ids.size() && flag; i++) {
			toInsert = new RowRecord();
			if (ids.get(i) != null) {
				toInsert.setId(ids.get(i).getValue());

				if (i < 32)
					toInsert.setClientName(names1.get(i).getValue());
				else if (i < 64)
					toInsert.setClientName(names2.get(i - 32).getValue());
				else if (i < 96)
					toInsert.setClientName(names3.get(i - 64).getValue());
				else if (i < 126)
					toInsert.setClientName(names4.get(i - 96).getValue());

				if (i < 42)
					toInsert.setPhoneNumber(phone1.get(i).getValue());
				else if (i < 84)
					toInsert.setPhoneNumber(phone2.get(i - 42).getValue());
				else if (i < 126)
					toInsert.setPhoneNumber(phone3.get(i - 84).getValue());
			} else
				flag = false;
			for (int j = 0; j < result.length; j++) {
				if (!result[j].isFull()) {
					result[j].insertRecord(toInsert);
					break;
				}
			}
		}

		return result;
	}

	// -- CALL THE FILE LEVEL TO FLUSH THE COLUMN PAGE (8 PAGES)

	private void writePagesToDesk(RowPage page, boolean flushToDesk, boolean evictFromBuffer) {
		ColumnPage toEvict = null;
		int offset = page.getOffset();

		Iterator<ColumnPage> iterator = columnPages.iterator();
		while (iterator.hasNext()) {
			toEvict = iterator.next();
			if (toEvict != null
					&& toEvict.getHashValue() == page.getHashValue()
					&& toEvict.getTableName() == page.getTableName()
					&& (toEvict.getOffset() == offset
					|| toEvict.getOffset() == offset * 4
					|| toEvict.getOffset() == offset * 4 + 1
					|| toEvict.getOffset() == offset * 4 + 2
					|| toEvict.getOffset() == offset * 4 + 3
					|| toEvict.getOffset() == offset * 3
					|| toEvict.getOffset() == offset * 3 + 1
					|| toEvict.getOffset() == offset * 3 + 2)) {
				if (flushToDesk) {
					fileManager.addPage(toEvict);
				}
				if (evictFromBuffer) {
					iterator.remove();
				}
			}
		}
		Collections.sort(columnPages);
	}

	// -- INCREMENT THE COUNTER OF USAGE FOR A PAGE (8 PAGES)
	private void incrementRowPageCounter(RowPage[] set) {
		for (int i = 0; i < 8; i++) {
			set[i].setUsageCounter(set[i].getUsageCounter() + 1);
		}
	}

	private void incrementColumnPageCounter(RowPage page) {
		ColumnPage cPage = null;
		int offset = page.getOffset();

		for (int i = 0; i < columnPages.size(); i++) {
			cPage = columnPages.get(i);
			if (cPage.getTableName().equals(page.getTableName())
					&& cPage.getHashValue() == page.getHashValue()) {
				if (cPage.getOffset() == offset
						|| cPage.getOffset() == offset * 4
						|| cPage.getOffset() == offset * 4 + 1
						|| cPage.getOffset() == offset * 4 + 2
						|| cPage.getOffset() == offset * 4 + 3
						|| cPage.getOffset() == offset * 3
						|| cPage.getOffset() == offset * 3 + 1
						|| cPage.getOffset() == offset * 3 + 2) {
					cPage.setUsageCounter(cPage.getUsageCounter() + 1);
				}
			}
		}
		Collections.sort(columnPages);
	}

	// -- INSERT ROW RECORD INTO COLUMN PAGE (8 PAGES)

	private void insertRowRecordIntoColumnPage(RowRecord record, RowPage page,
			int idLocation) {
		ColumnPage cPage = null;
		int offset = page.getOffset();

		for (int i = 0; i < columnPages.size(); i++) {
			cPage = columnPages.get(i);
			if (cPage.getTableName().equals(page.getTableName())
					&& cPage.getHashValue() == page.getHashValue()) {
				if (cPage.getOffset() == offset
						&& cPage.getColumnName().equals(Constant.id)) {
					cPage.insertValue(record.getId());
					cPage.setUsageCounter(cPage.getUsageCounter() + 1);
					cPage.setDirtyBit(true);

				} else if (cPage.getOffset() == offset * 4 + (idLocation / 32)
						&& cPage.getColumnName().equals(Constant.clientName)) {
					cPage.insertValue(record.getClientName());
					cPage.setUsageCounter(cPage.getUsageCounter() + 1);
					cPage.setDirtyBit(true);
				} else if (cPage.getOffset() == offset * 3 + (idLocation / 42)
						&& cPage.getColumnName().equals(Constant.phoneNumber)) {
					cPage.insertValue(record.getPhoneNumber());
					cPage.setUsageCounter(cPage.getUsageCounter() + 1);
					cPage.setDirtyBit(true);
				}
			}
		}
		Collections.sort(columnPages);
	}

	// -- INSERT ROW RECORD INTO NEWLY CREATED COLUMN PAGE (8 PAGES)
	private void insertRowRecordIntoNewColumnPage(RowPage[] subset,
			RowRecord record) {
		holdInTemp = new ColumnPage[8];

		holdInTemp[0] = new ColumnPage(Constant.id, subset[0].getTableName(),
				subset[0].getHashValue(), subset[0].getOffset(), 1, true, 0);
		for (int i = 0; i < 4; i++) {
			holdInTemp[i + 1] = new ColumnPage(Constant.clientName,
					subset[0].getTableName(), subset[0].getHashValue(),
					subset[0].getOffset() * 4 + i, 1, true, i);
		}
		for (int i = 0; i < 3; i++) {
			holdInTemp[i + 5] = new ColumnPage(Constant.phoneNumber,
					subset[0].getTableName(), subset[0].getHashValue(),
					subset[0].getOffset() * 3 + i, 1, true, i);
		}

		if (record != null) {
			holdInTemp[0].insertValue(record.getId());
			holdInTemp[1].insertValue(record.getClientName());
			holdInTemp[5].insertValue(record.getPhoneNumber());
		}
	}

	// -- REMOVE RECORD FROM PAGES
	//TODO -- test
	private void removeRecordFromPages(String recordID,
			ColumnPage[] columnPages, RowPage[] rowPages) {
		for(int i = 0; i < 8; i++)
			rowPages[i].deleteRecord(recordID);

		for (int i = 0; i < columnPages[0].getRecords().size(); i++) {
			if (columnPages[0].getRecords().get(i).getValue().equals(recordID)) {
				columnPages[0].getRecords().remove(i);

				if (i < 32)
					columnPages[1].getRecords().remove(i);
				else if (i < 64)
					columnPages[2].getRecords().remove(i - 32);
				else if (i < 96)
					columnPages[3].getRecords().remove(i - 64);
				else if (i < 126)
					columnPages[4].getRecords().remove(i - 96);

				if (i < 42)
					columnPages[5].getRecords().remove(i);
				else if (i < 84)
					columnPages[6].getRecords().remove(i - 42);
				else if (i < 126)
					columnPages[7].getRecords().remove(i - 84);
			}
		}
		System.out.println();
	}

	// -- ACTUAL DELETE TABLEF FROM BUFFER

	//TODO -- test
	private String actualDeleteTableFromBuffer(String tableName) {
		String result = "done";
		RowPage toDeleteRow = null;
		ColumnPage toDeleteColumn = null;

		Iterator<RowPage> rowIterator = rowPages.iterator();
		while (rowIterator.hasNext()) {
			toDeleteRow = rowIterator.next();
			if (toDeleteRow.getTableName().equals(tableName)) {
				rowIterator.remove();
			}
		}
		Collections.sort(rowPages);

		Iterator<ColumnPage> columnIterator = columnPages.iterator();
		while (columnIterator.hasNext()) {
			toDeleteColumn = columnIterator.next();
			if (toDeleteColumn.getTableName().equals(tableName)) {
				columnIterator.remove();
			}
		}
		Collections.sort(columnPages);

		return result;
	}
}
