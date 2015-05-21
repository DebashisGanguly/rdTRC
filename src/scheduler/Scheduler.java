package rdtrc.scheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import rdtrc.global.Constant;
import rdtrc.global.Logger;
import rdtrc.interfaceManagement.AccessInterface;
import rdtrc.structures.LockEntity;
import rdtrc.structures.WaitForGraph;

public class Scheduler {
	private Logger logger;
	private AccessInterface accessInterface = null;
	private static Scheduler instance = null;
	private HashMap<Long, LinkedList<LockEntity>> lockTable;
	private HashMap<Long, LinkedList<LockEntity>> waitBuffer;
	private ArrayList<Long> abortedTransactions;
	private WaitForGraph WFG;

	private Scheduler() {
		logger = Logger.getInstance();
		accessInterface = AccessInterface.getInstance();
		lockTable = new HashMap<Long, LinkedList<LockEntity>>();
		waitBuffer = new HashMap<Long, LinkedList<LockEntity>>();
		abortedTransactions = new ArrayList<Long>();
		WFG = new WaitForGraph();
	}

	public static Scheduler getInstance() {
		if (instance == null) {
			instance = new Scheduler();
		}
		return instance;
	}

	public void scheduleOperation(LockEntity newLockEntity) {
		if (abortedTransactions.contains(newLockEntity
				.getTransactionTimestamp())) {
			System.out.println("Ignored " + newLockEntity.toString() + " as T:"
					+ newLockEntity.getTransactionTimestamp() + " is aborted.");
			logger.WriteLog("Ignored " + newLockEntity.toString() + " as T:"
					+ newLockEntity.getTransactionTimestamp() + " is aborted.");
			return;
		}

		boolean isBlocked = WFG.isBlocked(newLockEntity
				.getTransactionTimestamp());

		if (isBlocked) {
			// Add to waiting buffer
			addToWaitingBuffer(newLockEntity);
		} else {
			long toConflictingItemLockTable = isConflictingItemPresentInLockTable(newLockEntity);
			// long toConflictingItemWaitBuffer =
			// isConflictingItemPresentInWaitBuffer(newLockEntity);

			long toConflictingItem = toConflictingItemLockTable;/*
																 * toConflictingItemLockTable
																 * != -1 ?
																 * toConflictingItemLockTable
																 * :
																 * toConflictingItemWaitBuffer
																 * ;
																 */

			if (toConflictingItem != -1) {
				WFG.addNode(newLockEntity.getTransactionTimestamp());
				boolean isDeadLock = WFG.buildEdge(
						newLockEntity.getTransactionTimestamp(),
						toConflictingItem, newLockEntity.getTableName());

				if (!isDeadLock) {
					// Add to waiting buffer
					addToWaitingBuffer(newLockEntity);
				} else {
					// current item is deadlock victim, abort
					System.out.println("Aborted T:"
							+ newLockEntity.getTransactionTimestamp()
							+ " as detected to be deadlock victim.");
					logger.WriteLog("Aborted T:"
							+ newLockEntity.getTransactionTimestamp()
							+ " as detected to be deadlock victim.");

					acknowledge(newLockEntity.getTransactionTimestamp(), false);
				}
			} else {
				// Add to lock table
				addToLockTable(newLockEntity);

				// pass to dm
				passToDataManager(newLockEntity);
			}
		}
	}

	private void passToDataManager(LockEntity newLockEntity) {
		System.out.println("Passed to data manager:: "
				+ newLockEntity.toString());
		logger.WriteLog("Passed to data manager:: " + newLockEntity.toString());

		if (newLockEntity.getOperation().equalsIgnoreCase(
				Constant.commitCommand)) {
			accessInterface.acknowledgeCompletion(
					newLockEntity.getTransactionTimestamp(), true);
			acknowledge(newLockEntity.getTransactionTimestamp(), true);
		} else if (newLockEntity.getOperation().equalsIgnoreCase(
				Constant.abortCommand)) {
			accessInterface.acknowledgeCompletion(
					newLockEntity.getTransactionTimestamp(), false);
			acknowledge(newLockEntity.getTransactionTimestamp(), false);
		} else {
			if (!accessInterface.passOperationToDataManager(new LockEntity(
					newLockEntity))) {
				if (newLockEntity.isTransaction()) {
					// abort;
					System.out.println("Aborted T:"
							+ newLockEntity.getTransactionTimestamp() + " as "
							+ newLockEntity.toString()
							+ " has violated integrity constraint.");
					logger.WriteLog("Aborted T:"
							+ newLockEntity.getTransactionTimestamp() + " as "
							+ newLockEntity.toString()
							+ " has violated integrity constraint.");

					accessInterface.acknowledgeCompletion(
							newLockEntity.getTransactionTimestamp(), false);
					acknowledge(newLockEntity.getTransactionTimestamp(), false);
					return;
				}
			} else {
				if (!newLockEntity.isTransaction()) {
					accessInterface.acknowledgeCompletion(
							newLockEntity.getTransactionTimestamp(), true);
					acknowledge(newLockEntity.getTransactionTimestamp(), true);
				} else {
					WFG.addNode(newLockEntity.getTransactionTimestamp());
				}
			}
		}
	}

	private void acknowledge(long transactionTimestamp, boolean isCommit) {
		if (lockTable.containsKey(new Long(transactionTimestamp))) {
			lockTable.remove(new Long(transactionTimestamp));
		}

		if (!isCommit) {
			abortedTransactions.add(new Long(transactionTimestamp));

			if (waitBuffer.containsKey(new Long(transactionTimestamp))) {
				waitBuffer.remove(new Long(transactionTimestamp));
			}
		} else {
			List<Long> dependents = WFG.removeNode(transactionTimestamp);

			for (Iterator<Entry<Long, LinkedList<LockEntity>>> waitBufferIterator = waitBuffer
					.entrySet().iterator(); waitBufferIterator.hasNext();) {
				Entry<Long, LinkedList<LockEntity>> transactionEntry = (Entry<Long, LinkedList<LockEntity>>) waitBufferIterator
						.next();

				if (dependents.contains(new Long(transactionEntry.getKey()))) {
					for (Iterator<LockEntity> lockEntityIterator = transactionEntry
							.getValue().iterator(); lockEntityIterator
							.hasNext();) {
						LockEntity currentLockEntity = lockEntityIterator
								.next();

						// pass to dm
						passToDataManager(currentLockEntity);
					}

					dependents.remove(new Long(transactionEntry.getKey()));
				}
			}
		}
	}

	private void addToLockTable(LockEntity newLockEntity) {
		if (lockTable.containsKey(new Long(newLockEntity
				.getTransactionTimestamp()))) {
			System.out.println("Aquired lock by " + newLockEntity.toString());
			logger.WriteLog("Aquired lock by " + newLockEntity.toString());

			lockTable.get(new Long(newLockEntity.getTransactionTimestamp()))
					.addLast(newLockEntity);
		} else {
			if (!(newLockEntity.getOperation().equalsIgnoreCase(
					Constant.abortCommand) || newLockEntity.getOperation()
					.equalsIgnoreCase(Constant.commitCommand))) {
				LinkedList<LockEntity> lockList = new LinkedList<LockEntity>();
				lockList.add(newLockEntity);
				lockTable.put(
						new Long(newLockEntity.getTransactionTimestamp()),
						lockList);
			}
		}
	}

	private void addToWaitingBuffer(LockEntity newLockEntity) {
		System.out.println("Added " + newLockEntity.toString()
				+ " to waiting queue");
		logger.WriteLog("Added " + newLockEntity.toString()
				+ " to waiting queue");

		if (waitBuffer.containsKey(new Long(newLockEntity
				.getTransactionTimestamp()))) {
			waitBuffer.get(new Long(newLockEntity.getTransactionTimestamp()))
					.addLast(newLockEntity);
		} else {
			LinkedList<LockEntity> waitList = new LinkedList<LockEntity>();
			waitList.add(newLockEntity);
			waitBuffer.put(new Long(newLockEntity.getTransactionTimestamp()),
					waitList);
		}
	}

	private long isConflictingItemPresentInLockTable(LockEntity newLockEntity) {
		long toConflictingItem = -1;

		for (Iterator<Entry<Long, LinkedList<LockEntity>>> lockTableIterator = lockTable
				.entrySet().iterator(); lockTableIterator.hasNext();) {
			Entry<Long, LinkedList<LockEntity>> transactionEntry = (Entry<Long, LinkedList<LockEntity>>) lockTableIterator
					.next();

			if (newLockEntity.getTransactionTimestamp() != transactionEntry
					.getKey()) {
				for (Iterator<LockEntity> lockEntityIterator = transactionEntry
						.getValue().iterator(); lockEntityIterator.hasNext();) {
					LockEntity currentLockEntity = lockEntityIterator.next();
					if (currentLockEntity.isTransaction()
							&& currentLockEntity
									.isConflictingEntity(newLockEntity)) {
						toConflictingItem = currentLockEntity
								.getTransactionTimestamp();
					}
				}
			}
		}

		return toConflictingItem;
	}

	private long isConflictingItemPresentInWaitBuffer(LockEntity newLockEntity) {
		long toConflictingItem = -1;

		for (Iterator<Entry<Long, LinkedList<LockEntity>>> waitBufferIterator = waitBuffer
				.entrySet().iterator(); waitBufferIterator.hasNext();) {
			Entry<Long, LinkedList<LockEntity>> transactionEntry = (Entry<Long, LinkedList<LockEntity>>) waitBufferIterator
					.next();

			if (newLockEntity.getTransactionTimestamp() != transactionEntry
					.getKey()) {
				for (Iterator<LockEntity> lockEntityIterator = transactionEntry
						.getValue().iterator(); lockEntityIterator.hasNext();) {
					LockEntity currentLockEntity = lockEntityIterator.next();
					if (currentLockEntity.isTransaction()
							&& currentLockEntity
									.isConflictingEntity(newLockEntity)) {
						toConflictingItem = currentLockEntity
								.getTransactionTimestamp();
					}
				}
			}
		}

		return toConflictingItem;
	}

	public void flushStructuresToDisk() {
		accessInterface.storeIndexes();
		accessInterface.storeTables();
		accessInterface.storeSecondayIndex();
	}

	public ArrayList<Long> getAbortedTransactions() {
		return abortedTransactions;
	}
}
