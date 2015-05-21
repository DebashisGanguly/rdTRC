package rdtrc.fileManagement;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Scanner;

import rdtrc.global.Constant;
import rdtrc.structures.ColumnPage;
import rdtrc.structures.ColumnRecord;
import rdtrc.structures.SecondaryIndex;
import rdtrc.structures.TableIndex;
import rdtrc.structures.Tuple;
import rdtrc.structures.ConcurrentReadingMode;

public class FileManagement {

	private static FileManagement instance = null;

	public static FileManagement getInstance() {
		if (instance == null) {
			instance = new FileManagement();
		}
		return instance;
	}

	public void deleteTable(String tableName) {
		String idFileName = Constant.fileFolder + "/" + tableName + "."
				+ Constant.id;
		deleteFile(idFileName);

		String clinetNameFileName = Constant.fileFolder + "/" + tableName + "."
				+ Constant.clientName;
		deleteFile(clinetNameFileName);

		String phoneNumberFileName = Constant.fileFolder + "/" + tableName
				+ "." + Constant.phoneNumber;
		deleteFile(phoneNumberFileName);

		String indexFileName = Constant.fileFolder + "/" + tableName + "."
				+ Constant.index;
		deleteFile(indexFileName);

		String secondaryIndexFileName = Constant.fileFolder + "/" + tableName
				+ "." + Constant.secondaryIndex;
		deleteFile(secondaryIndexFileName);
	}

	private void deleteFile(String fileName) {
		File dataFile = new File(fileName);

		if (!dataFile.exists()) {
			return;
		}

		dataFile.delete();
	}

	public ColumnPage fetchPage(String tableName, String columnName,
			int hashValue, int pageOffset) {
		int index = 0;
		if (columnName.equals(Constant.clientName)) {
			if ((pageOffset % 4) == 3)
				index = 3;
		}

		ColumnPage columnPage = new ColumnPage(columnName, tableName,
				hashValue, pageOffset, 0, false, index);

		File mainDirectory = new File(Constant.fileFolder);

		if (!mainDirectory.exists() || !mainDirectory.isDirectory()) {
			mainDirectory.mkdirs();
		}

		String fileName = Constant.fileFolder + "/" + columnPage.getTableName()
				+ "." + columnPage.getColumnName();

		File dataFile = new File(fileName);

		if (!dataFile.exists()) {
			try {
				dataFile.createNewFile();
			} catch (IOException e) {
			}
		}

		List<String> lines = null;

		try {
			lines = Files.readAllLines(dataFile.toPath(),
					Charset.defaultCharset());
		} catch (IOException e) {

		}

		String readLine = "";

		if (lines != null) {
			for (int i = 0; i < lines.size(); i++) {
				if (i == pageOffset) {
					readLine = lines.get(i);
					break;
				}
			}
		}

		List<String> stringRecords = Arrays.asList(readLine.split(","));
		List<ColumnRecord> columnRecords = columnPage.getRecords();

		for (String item : stringRecords) {
			if (item != null && !item.isEmpty())
				columnRecords.add(new ColumnRecord(item));
		}

		return columnPage;
	}

	public void addPage(ColumnPage columnPage) {

		File mainDirectory = new File(Constant.fileFolder);

		if (!mainDirectory.exists() || !mainDirectory.isDirectory()) {
			mainDirectory.mkdirs();
		}

		String fileName = Constant.fileFolder + "/" + columnPage.getTableName()
				+ "." + columnPage.getColumnName();

		File dataFile = new File(fileName);

		if (!dataFile.exists()) {
			try {
				dataFile.createNewFile();
			} catch (IOException e) {
			}
		}

		List<String> lines = null;

		try {
			lines = Files.readAllLines(dataFile.toPath(),
					Charset.defaultCharset());
		} catch (IOException e) {

		}

		try {
			PrintWriter output = new PrintWriter(new FileWriter(fileName));

			int maxLoop = -1;
			if (lines.size() <= columnPage.getOffset()) {
				maxLoop = columnPage.getOffset() + 1;
			} else {
				maxLoop = lines.size();
			}

			for (int i = 0; i < maxLoop; i++) {
				String line = "";

				if (i == columnPage.getOffset()) {
					line = columnPage.toString();
				} else if (i < lines.size()) {
					line = lines.get(i);
				}

				if (line == null || line.isEmpty()) {
					output.println();
				} else {
					output.println(line);
				}
			}

			output.close();
		} catch (IOException e) {
		}
	}

	public void writeIndex(HashMap<String, TableIndex> indices) {

		File mainDirectory = new File(Constant.fileFolder);

		if (!mainDirectory.exists() || !mainDirectory.isDirectory()) {
			mainDirectory.mkdirs();
		}

		for (Entry<String, TableIndex> index : indices.entrySet()) {

			String fileName = Constant.fileFolder + "/" + index.getKey() + "."
					+ Constant.index;

			File indexFile = new File(fileName);

			if (!indexFile.exists()) {
				try {
					indexFile.createNewFile();
				} catch (IOException e) {
				}
			}

			try {
				PrintWriter output = new PrintWriter(new FileWriter(fileName));

				output.println(index.getValue().toString());
				output.close();
			} catch (IOException e) {
			}
		}
	}

	public HashMap<String, TableIndex> readIndex() {
		HashMap<String, TableIndex> indices = new HashMap<String, TableIndex>();

		File mainDirectory = new File(Constant.fileFolder);

		if (!mainDirectory.exists() || !mainDirectory.isDirectory()) {
			mainDirectory.mkdirs();
		}

		File[] directoryListing = mainDirectory.listFiles();

		if (directoryListing != null) {
			for (File indexFile : directoryListing) {

				String path = indexFile.getPath();

				String tableName = indexFile.getName().substring(0,
						indexFile.getName().lastIndexOf("."));
				String ext = path.substring(path.lastIndexOf(".") + 1,
						path.length());

				if (!ext.equals(Constant.index)) {
					continue;
				}

				String readLine = "";

				try {
					Scanner scanner = new Scanner(indexFile);

					TableIndex index = new TableIndex(tableName);
					HashMap<Integer, List<Integer>> offsets = index
							.getOffsets();

					while (scanner.hasNext()) {
						readLine = scanner.next();

						int hashBucket = Integer.parseInt(readLine.substring(0,
								readLine.indexOf('|')));

						List<String> offsetStringList = Arrays.asList(readLine
								.substring(readLine.indexOf('|') + 1,
										readLine.length()).split(","));

						List<Integer> offsetList = new ArrayList<Integer>();

						for (String item : offsetStringList) {
							offsetList.add(Integer.parseInt(item));
						}

						offsets.put(hashBucket, offsetList);
					}

					index.setOffsets(offsets);
					indices.put(tableName, index);
					scanner.close();
				} catch (FileNotFoundException e) {
				}
			}
		}

		return indices;
	}

	public void populateConfigurationItems() {
		Constant.bufferSize = -1;
		Constant.randomSeed = -1;
		int readingMode = -1;

		boolean errorInConfig = false;

		File mainDirectory = new File(Constant.fileFolder);

		if (!mainDirectory.exists() || !mainDirectory.isDirectory()) {
			mainDirectory.mkdirs();
		}

		String fileName = Constant.fileFolder + "/"
				+ Constant.configurationFileName;

		File dataFile = new File(fileName);

		String readLine = "";

		try {
			Scanner scanner = new Scanner(dataFile);
			while (scanner.hasNext()) {
				readLine = scanner.next();
				if (readLine.indexOf(Constant.bufferSizeConfigurationItem) != -1) {
					try {
						Constant.bufferSize = Integer.parseInt(readLine
								.substring(Constant.bufferSizeConfigurationItem
										.length()));
					} catch (NumberFormatException ex) {
					}
				} else if (readLine
						.indexOf(Constant.concurrentReadingModeConfigurationItem) != -1) {
					try {
						readingMode = Integer
								.parseInt(readLine
										.substring(Constant.concurrentReadingModeConfigurationItem
												.length()));
					} catch (NumberFormatException ex) {
					}
				} else if (readLine
						.indexOf(Constant.randomSeedConfigurationItem) != -1) {
					try {
						Constant.randomSeed = Long.parseLong(readLine
								.substring(Constant.randomSeedConfigurationItem
										.length()));
					} catch (NumberFormatException ex) {
					}
				}
			}
			scanner.close();
		} catch (FileNotFoundException e) {
		}

		if (Constant.bufferSize == -1) {
			System.out
					.println("Cannot start the system. Error with buffer size in configuration file.");
			errorInConfig = true;
		} else if (Constant.bufferSize < 16) {
			System.out
					.println("Minimum buffer size to run the system is 16 pages (8 pages each for both the stores). Considering the minimum size and proceeding.");
			Constant.bufferSize = 16;
		} else {
			System.out
					.println("Buffer size to run the system should be in multiples of 16 pages (8 pages each for both the stores). Considering the size as nearest multiple of 16 of specified size and proceeding.");
			if (Constant.bufferSize % 16 > 7) {
				Constant.bufferSize = Constant.bufferSize
						+ (16 - Constant.bufferSize % 16);
			} else {
				Constant.bufferSize = Constant.bufferSize - Constant.bufferSize
						% 16;
			}
		}

		if (readingMode == 0) {
			Constant.readingMode = ConcurrentReadingMode.RoundRobin;
		} else if (readingMode == 1) {
			Constant.readingMode = ConcurrentReadingMode.Random;
		} else {
			System.out
					.println("Cannot start the system. Error with concurrent reading mode in configuration file.");
			errorInConfig = true;
		}

		if (Constant.randomSeed <= 0) {
			System.out
					.println("Cannot start the system. Error with random seed in configuration file.");
			errorInConfig = true;
		}

		if (errorInConfig) {
			System.out
					.println("Exiting the system. Please fix the configuration file and start again.");
			System.exit(0);
		}
	}

	public void writeSecondaryIndex(HashMap<String, SecondaryIndex> indices) {
		File mainDirectory = new File(Constant.fileFolder);

		if (!mainDirectory.exists() || !mainDirectory.isDirectory()) {
			mainDirectory.mkdirs();
		}

		for (Entry<String, SecondaryIndex> index : indices.entrySet()) {

			String fileName = Constant.fileFolder + "/" + index.getKey() + "."
					+ Constant.secondaryIndex;

			File indexFile = new File(fileName);

			if (!indexFile.exists()) {
				try {
					indexFile.createNewFile();
				} catch (IOException e) {
				}
			}

			try {
				PrintWriter output = new PrintWriter(new FileWriter(fileName));

				StringBuilder sb = new StringBuilder();
				SecondaryIndex tableIndex = index.getValue();

				for (int areaCode : tableIndex.levelOrder()) {
					sb.append(areaCode);
					sb.append("|");

					HashMap<Tuple<Integer, Integer>, List<Integer>> offsetList = tableIndex
							.get(areaCode);

					for (Entry<Tuple<Integer, Integer>, List<Integer>> offset : offsetList
							.entrySet()) {
						sb.append(offset.getKey().getLeft() + "-"
								+ offset.getKey().getRight());
						sb.append(":");

						for (Integer recordPosition : offset.getValue()) {
							sb.append(recordPosition);
							sb.append(",");
						}

						sb.deleteCharAt(sb.length() - 1);
						sb.append("|");
					}

					sb.deleteCharAt(sb.length() - 1);
					sb.append(System.getProperty("line.separator"));
				}

				output.println(sb.toString());
				output.close();
			} catch (IOException e) {
			}
		}
	}

	public HashMap<String, SecondaryIndex> readSecondaryIndex() {
		HashMap<String, SecondaryIndex> secondaryIndices = new HashMap<String, SecondaryIndex>();

		File mainDirectory = new File(Constant.fileFolder);

		if (!mainDirectory.exists() || !mainDirectory.isDirectory()) {
			mainDirectory.mkdirs();
		}

		File[] directoryListing = mainDirectory.listFiles();

		if (directoryListing != null) {
			for (File indexFile : directoryListing) {

				String path = indexFile.getPath();

				String tableName = indexFile.getName().substring(0,
						indexFile.getName().lastIndexOf("."));
				String ext = path.substring(path.lastIndexOf(".") + 1,
						path.length());

				if (!ext.equals(Constant.secondaryIndex)) {
					continue;
				}

				String readLine = "";

				try {
					Scanner scanner = new Scanner(indexFile);

					SecondaryIndex index = new SecondaryIndex(tableName);
					HashMap<Tuple<Integer, Integer>, List<Integer>> offsets;

					while (scanner.hasNext()) {
						readLine = scanner.next();

						if (readLine != null && !readLine.isEmpty()) {
							List<String> lineSplit = Arrays.asList(readLine
									.split("\\|"));

							if (lineSplit.size() >= 2) {

								offsets = new HashMap<Tuple<Integer, Integer>, List<Integer>>();

								int keyAreaCode = Integer.parseInt(lineSplit
										.get(0));

								for (int i = 1; i < lineSplit.size(); i++) {
									String offsetLine = lineSplit.get(i);

									List<String> offsetSplit = Arrays
											.asList(offsetLine.split(":"));

									if (offsetSplit.size() == 2) {
										List<Integer> recordList = new ArrayList<Integer>();

										String hashOffset = offsetSplit.get(0);

										List<String> hashOffsetSplit = Arrays
												.asList(hashOffset.split("-"));

										int hashValue = Integer
												.parseInt(hashOffsetSplit
														.get(0));
										int offset = Integer
												.parseInt(hashOffsetSplit
														.get(1));

										List<String> recordSplit = Arrays
												.asList(offsetSplit.get(1)
														.split(","));

										for (int j = 0; j < recordSplit.size(); j++) {
											int recordPosition = Integer
													.parseInt(recordSplit
															.get(j));

											recordList.add(recordPosition);
										}

										offsets.put(
												new Tuple<Integer, Integer>(
														hashValue, offset),
												recordList);
									}
								}

								index.put(keyAreaCode, offsets);
							}
						}
					}

					secondaryIndices.put(tableName, index);
					scanner.close();
				} catch (FileNotFoundException e) {
				}
			}
		}

		return secondaryIndices;
	}
}
