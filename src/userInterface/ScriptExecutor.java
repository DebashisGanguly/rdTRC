package rdtrc.userInterface;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.regex.Matcher;

import rdtrc.global.Constant;
import rdtrc.global.Logger;
import rdtrc.scheduler.Scheduler;
import rdtrc.structures.LockEntity;
import rdtrc.structures.ScriptBookmark;
import rdtrc.structures.Tuple;
import rdtrc.structures.ConcurrentReadingMode;

public class ScriptExecutor {

	private static ScriptExecutor instance = null;

	private Logger logger;
	private Scheduler scheduler;

	private Random randomGenerator;

	LinkedHashMap<String, Tuple<ScriptBookmark, List<String>>> scripts;
	long transactionCounter = 1;
	long transactionTimestamp = 1;

	private int readOperations = 0;
	private int writeOperations = 0;
	private Date startTime;
	private Date endTime;
	SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static ScriptExecutor getInstance() {
		if (instance == null) {
			instance = new ScriptExecutor();
		}
		return instance;
	}

	private ScriptExecutor() {
		logger = Logger.getInstance();
		scheduler = Scheduler.getInstance();

		randomGenerator = new Random(Constant.randomSeed);
		scripts = new LinkedHashMap<String, Tuple<ScriptBookmark, List<String>>>();
	}

	public void processUserInput(String path) {
		File fileInput = new File(path);

		if (!fileInput.exists() || !fileInput.canRead()) {
			return;
		}

		if (fileInput.isDirectory()) {
			File[] directoryListing = fileInput.listFiles();
			Arrays.sort(directoryListing);

			if (directoryListing != null) {
				for (File child : directoryListing) {
					String childPath = child.getPath();

					String ext = childPath.substring(
							childPath.lastIndexOf(".") + 1, childPath.length());

					if (!ext.equals(Constant.scriptExtension)) {
						continue;
					} else {
						try {
							List<String> scriptLines = Files.readAllLines(
									child.toPath(), Charset.defaultCharset());
							Tuple<ScriptBookmark, List<String>> scriptValue = new Tuple<ScriptBookmark, List<String>>(
									new ScriptBookmark(), scriptLines);

							scripts.put(child.getName(), scriptValue);
						} catch (IOException e) {

						}
					}
				}
			}
		} else {
			if (fileInput.isFile()) {
				try {
					List<String> scriptLines = Files.readAllLines(
							fileInput.toPath(), Charset.defaultCharset());
					Tuple<ScriptBookmark, List<String>> scriptValue = new Tuple<ScriptBookmark, List<String>>(
							new ScriptBookmark(), scriptLines);

					scripts.put(fileInput.getName(), scriptValue);
				} catch (IOException e) {

				}
			}
		}

		startTime = new Date();

		scheduleOperation();

		endTime = new Date();

		printStatistics();
	}

	private void printStatistics() {
		System.out.println("=====================================");
		System.out.println("Statistics of the run");
		System.out.println("=====================================");
		System.out.println("Total read operations: " + readOperations);
		System.out
				.println("Percentage of read operations: "
						+ (long) (readOperations * 100 / (readOperations + writeOperations))
						+ "%");
		System.out.println("Total write operations: " + writeOperations);
		System.out
				.println("Percentage of write operations: "
						+ (long) (writeOperations * 100 / (readOperations + writeOperations))
						+ "%");
		System.out.println("Total number of transactions and processes: "
				+ (transactionCounter - 1));
		System.out
				.println("Total number of transactions and processes aborted: "
						+ scheduler.getAbortedTransactions().size());
		System.out.println("Processing started at: "
				+ sdfDate.format(startTime));
		System.out.println("Processing ended at: " + sdfDate.format(endTime));
		System.out.println("Total time elapsed (milliseconds): "
				+ (endTime.getTime() - startTime.getTime()));
		System.out
				.println("Average response time (goodput) of executing transactions (milliseconds): "
						+ (endTime.getTime() - startTime.getTime())
						/ ((transactionCounter - 1) - scheduler
								.getAbortedTransactions().size()));
	}

	public void scheduleOperation() {
		String logEntry = "";
		if (Constant.readingMode == ConcurrentReadingMode.RoundRobin) {

			while (!scripts.isEmpty()) {
				Iterator scriptIterator = scripts.entrySet().iterator();

				while (scriptIterator.hasNext()) {
					Entry<String, Tuple<ScriptBookmark, List<String>>> scriptEntry = (Entry<String, Tuple<ScriptBookmark, List<String>>>) scriptIterator
							.next();
					String fileName = scriptEntry.getKey();
					List<String> scriptLines = (List<String>) scriptEntry
							.getValue().getRight();
					ScriptBookmark bookmark = (ScriptBookmark) scriptEntry
							.getValue().getLeft();

					if (scriptLines.size() == 0) {
						logEntry = "Script " + fileName + " is empty.";
						System.out.println(logEntry);
						logger.WriteLog(logEntry);
						scriptIterator.remove();
					} else if (bookmark.getLineNumber() == -1) {
						logEntry = "Started processing script " + fileName
								+ ".";
						System.out.println(logEntry);
						logger.WriteLog(logEntry);
						parseScriptLine(
								scriptLines.get(bookmark.getLineNumber() + 1),
								bookmark);
					} else if (bookmark.getLineNumber() == scriptLines.size() - 1) {
						logEntry = "Reached end of script " + fileName + ".";
						System.out.println(logEntry);
						logger.WriteLog(logEntry);
						scriptIterator.remove();
					} else {
						parseScriptLine(
								scriptLines.get(bookmark.getLineNumber() + 1),
								bookmark);
					}
				}
			}
		} else {
			while (!scripts.isEmpty()) {
				int randomFilePosition = Math.abs(randomGenerator.nextInt())
						% scripts.size();

				List<String> fileNames = new ArrayList(scripts.keySet());

				String fileName = fileNames.get(randomFilePosition);

				List<String> scriptLines = (List<String>) scripts.get(fileName)
						.getRight();
				ScriptBookmark bookmark = (ScriptBookmark) scripts
						.get(fileName).getLeft();

				if (scriptLines.size() == 0) {
					logEntry = "Script " + fileName + " is empty.";
					System.out.println(logEntry);
					logger.WriteLog(logEntry);
					scripts.remove(fileName);
				} else if (bookmark.getLineNumber() == -1) {
					logEntry = "Started processing script " + fileName + ".";
					System.out.println(logEntry);
					logger.WriteLog(logEntry);

					int randomLineNumbers = Math.abs(randomGenerator.nextInt())
							% (scriptLines.size() - bookmark.getLineNumber() - 1);

					for (int i = bookmark.getLineNumber() + 1; i < Math.min(
							bookmark.getLineNumber() + 1 + randomLineNumbers,
							scriptLines.size()); i++)
						parseScriptLine(scriptLines.get(i), bookmark);
				} else if (bookmark.getLineNumber() == scriptLines.size() - 1) {
					logEntry = "Reached end of script " + fileName + ".";
					System.out.println(logEntry);
					logger.WriteLog(logEntry);
					scripts.remove(fileName);
				} else {
					System.out.println(scriptLines.size()
							- bookmark.getLineNumber() - 1);
					System.out.println(scriptLines.size());
					System.out.println(bookmark.getLineNumber());
					int randomLineNumbers = Math.abs(randomGenerator.nextInt())
							% (scriptLines.size() - bookmark.getLineNumber() - 1);

					for (int i = bookmark.getLineNumber() + 1; i < Math.min(
							bookmark.getLineNumber() + 1 + randomLineNumbers,
							scriptLines.size()); i++)
						parseScriptLine(
								scriptLines.get(bookmark.getLineNumber() + 1),
								bookmark);
				}
			}
		}
	}

	public void parseScriptLine(String line, ScriptBookmark bookmark) {
		bookmark.setLineNumber(bookmark.getLineNumber() + 1);

		final Matcher matcherCommandLine = Constant.scriptParsingPattern
				.matcher("");
		final Matcher matcherInsertArgument = Constant.insertArgumentParsingPattern
				.matcher("");

		String processedLine = "";

		matcherCommandLine.reset(line);

		while (matcherCommandLine.find()) {
			processedLine = matcherCommandLine.group();
		}

		if (processedLine.isEmpty()) {
			System.out.println("Bad command: " + line);
			logger.WriteLog("Bad command: " + line);
			return;
		}

		List<String> literalList = Arrays.asList(processedLine.split("[\\s]+"));
		String command = literalList.get(0);

		if (command.equalsIgnoreCase(Constant.commitCommand)
				|| command.equalsIgnoreCase(Constant.abortCommand)) {
			if (bookmark.getTimestamp() == -1) {
				System.out.println("No Begin command found; Command Ignored: "
						+ processedLine);
				logger.WriteLog("No Begin command found; Command Ignored: "
						+ processedLine);
				return;
			} else {
				if (command.equalsIgnoreCase(Constant.commitCommand)) {
					System.out
							.println(((bookmark.isTransaction() ? "T: " : "P: ")
									+ bookmark.getTransactionNumber() + " -> ")
									+ "Commit command");
					logger.WriteLog(((bookmark.isTransaction() ? "T: " : "P: ")
							+ bookmark.getTransactionNumber() + " -> ")
							+ "Commit command");

					if (bookmark.isTransaction()) {
						LockEntity newLockEntity = new LockEntity(
								bookmark.getTransactionNumber(),
								bookmark.isTransaction(),
								bookmark.isTransaction() ? bookmark
										.getTimestamp()
										: transactionTimestamp++,
								Constant.commitCommand, null, 0, null, null,
								null);

						scheduler.scheduleOperation(newLockEntity);
					}
				} else {
					System.out
							.println(((bookmark.isTransaction() ? "T: " : "P: ")
									+ bookmark.getTransactionNumber() + " -> ")
									+ "Abort command");
					logger.WriteLog(((bookmark.isTransaction() ? "T: " : "P: ")
							+ bookmark.getTransactionNumber() + " -> ")
							+ "Abort command");

					if (bookmark.isTransaction()) {
						LockEntity newLockEntity = new LockEntity(
								bookmark.getTransactionNumber(),
								bookmark.isTransaction(),
								bookmark.isTransaction() ? bookmark
										.getTimestamp()
										: transactionTimestamp++,
								Constant.abortCommand, null, 0, null, null,
								null);

						scheduler.scheduleOperation(newLockEntity);
					}
				}

				bookmark.setTransaction(false);
				bookmark.setTimestamp(-1);
			}
		} else if (command.equalsIgnoreCase(Constant.beginCommand)) {
			if (literalList.size() == 2) {
				try {
					int transactionMode = Integer.parseInt(literalList.get(1));

					if (transactionMode == 1) {
						System.out
								.println("Begin transaction command: Transaction Number: "
										+ transactionCounter);
						logger.WriteLog("Begin transaction command: Transaction Number: "
								+ transactionCounter);

						bookmark.setTransaction(true);
						bookmark.setTransactionNumber(transactionCounter++);
						bookmark.setTimestamp(transactionTimestamp++);
					} else {
						System.out
								.println("Begin process command: Process Number: "
										+ transactionCounter);
						logger.WriteLog("Begin process command: Process Number: "
								+ transactionCounter);

						bookmark.setTransactionNumber(transactionCounter++);
						bookmark.setTransaction(false);
						bookmark.setTimestamp(transactionTimestamp++);
					}
				} catch (Exception e) {
					System.out.println("Bad command: " + processedLine);
					logger.WriteLog("Bad command: " + processedLine);
					return;
				}
			} else {
				System.out.println("Bad command: " + processedLine);
				logger.WriteLog("Bad command: " + processedLine);
				return;
			}
		} else {
			String tableName = literalList.get(1);
			String argument = "";

			String logEntry = "";

			if (command.equalsIgnoreCase(Constant.deleteCommand)) {
				if (bookmark.getTimestamp() == -1) {
					System.out
							.println("No Begin command found; Command Ignored: "
									+ processedLine);
					logger.WriteLog("No Begin command found; Command Ignored: "
							+ processedLine);
					return;
				} else {
					System.out.println("Passed to scheduler:: "
							+ ((bookmark.isTransaction() ? "T: " : "P: ")
									+ bookmark.getTransactionNumber() + " -> ")
							+ processedLine);
					logger.WriteLog("Passed to scheduler:: "
							+ ((bookmark.isTransaction() ? "T: " : "P: ")
									+ bookmark.getTransactionNumber() + " -> ")
							+ processedLine);

					LockEntity newLockEntity = new LockEntity(
							bookmark.getTransactionNumber(),
							bookmark.isTransaction(),
							bookmark.isTransaction() ? bookmark.getTimestamp()
									: transactionTimestamp++,
							Constant.deleteCommand, tableName, 0, null, null,
							null);

					scheduler.scheduleOperation(newLockEntity);

					writeOperations++;
				}
			} else if (command.equalsIgnoreCase(Constant.insertCommand)) {
				String processedArgument = "";

				matcherInsertArgument.reset(processedLine);
				while (matcherInsertArgument.find()) {
					processedArgument = matcherInsertArgument.group();
				}

				if (processedArgument.isEmpty()) {
					System.out.println("Bad command: " + processedLine);
					logger.WriteLog("Bad command: " + processedLine);
					return;
				}

				List<String> attributeList = Arrays.asList(processedArgument
						.split("[\\s]*,[\\s]*"));

				String id = attributeList.get(0);
				String clientName = attributeList.get(1);
				String phoneNumber = attributeList.get(2);

				if (bookmark.getTimestamp() == -1) {
					System.out
							.println("No Begin command found; Command Ignored: "
									+ processedLine);
					logger.WriteLog("No Begin command found; Command Ignored: "
							+ processedLine);
					return;
				} else {
					System.out.println("Passed to scheduler:: "
							+ ((bookmark.isTransaction() ? "T: " : "P: ")
									+ bookmark.getTransactionNumber() + " -> ")
							+ processedLine);
					logger.WriteLog("Passed to scheduler:: "
							+ ((bookmark.isTransaction() ? "T: " : "P: ")
									+ bookmark.getTransactionNumber() + " -> ")
							+ processedLine);

					LockEntity newLockEntity = new LockEntity(
							bookmark.getTransactionNumber(),
							bookmark.isTransaction(),
							bookmark.isTransaction() ? bookmark.getTimestamp()
									: transactionTimestamp++,
							Constant.insertCommand, tableName,
							Integer.parseInt(id), clientName, phoneNumber,
							phoneNumber.substring(0, 3));

					scheduler.scheduleOperation(newLockEntity);
					writeOperations++;
				}
			} else {

				if (literalList.size() == 3) {
					argument = literalList.get(2);
				} else {
					System.out.println("Bad command: " + processedLine);
					logger.WriteLog("Bad command: " + processedLine);
					return;
				}

				if (bookmark.getTimestamp() == -1) {
					System.out
							.println("No Begin command found; Command Ignored: "
									+ processedLine);
					logger.WriteLog("No Begin command found; Command Ignored: "
							+ processedLine);
					return;
				}

				if (command.equalsIgnoreCase(Constant.retrieveByIdCommand)) {

					System.out.println("Passed to scheduler:: "
							+ ((bookmark.isTransaction() ? "T: " : "P: ")
									+ bookmark.getTransactionNumber() + " -> ")
							+ processedLine);
					logger.WriteLog("Passed to scheduler:: "
							+ ((bookmark.isTransaction() ? "T: " : "P: ")
									+ bookmark.getTransactionNumber() + " -> ")
							+ processedLine);

					LockEntity newLockEntity = new LockEntity(
							bookmark.getTransactionNumber(),
							bookmark.isTransaction(),
							bookmark.isTransaction() ? bookmark.getTimestamp()
									: transactionTimestamp++,
							Constant.retrieveByIdCommand, tableName,
							Integer.parseInt(argument), null, null, null);

					scheduler.scheduleOperation(newLockEntity);
					readOperations++;
				} else if (command
						.equalsIgnoreCase(Constant.retrieveByAreaCodeCommand)) {

					System.out.println("Passed to scheduler:: "
							+ ((bookmark.isTransaction() ? "T: " : "P: ")
									+ bookmark.getTransactionNumber() + " -> ")
							+ processedLine);
					logger.WriteLog("Passed to scheduler:: "
							+ ((bookmark.isTransaction() ? "T: " : "P: ")
									+ bookmark.getTransactionNumber() + " -> ")
							+ processedLine);

					LockEntity newLockEntity = new LockEntity(
							bookmark.getTransactionNumber(),
							bookmark.isTransaction(),
							bookmark.isTransaction() ? bookmark.getTimestamp()
									: transactionTimestamp++,
							Constant.retrieveByAreaCodeCommand, tableName, 0,
							null, null, argument);

					scheduler.scheduleOperation(newLockEntity);
					readOperations++;
				} else if (command
						.equalsIgnoreCase(Constant.groupByAreaCodeCountCommand)) {

					System.out.println("Passed to scheduler:: "
							+ ((bookmark.isTransaction() ? "T: " : "P: ")
									+ bookmark.getTransactionNumber() + " -> ")
							+ processedLine);
					logger.WriteLog("Passed to scheduler:: "
							+ ((bookmark.isTransaction() ? "T: " : "P: ")
									+ bookmark.getTransactionNumber() + " -> ")
							+ processedLine);

					LockEntity newLockEntity = new LockEntity(
							bookmark.getTransactionNumber(),
							bookmark.isTransaction(),
							bookmark.isTransaction() ? bookmark.getTimestamp()
									: transactionTimestamp++,
							Constant.groupByAreaCodeCountCommand, tableName, 0,
							null, null, argument);

					scheduler.scheduleOperation(newLockEntity);
					readOperations++;
				} else {
					System.out.println("Bad command: " + processedLine);
					logger.WriteLog("Bad command: " + processedLine);
					return;
				}
			}
		}
	}

	public void flushStructuresToDisk() {
		scheduler.flushStructuresToDisk();
	}
}
