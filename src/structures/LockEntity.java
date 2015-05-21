package rdtrc.structures;

import rdtrc.global.Constant;

public class LockEntity {
	private long transactionNumber;
	private boolean isTransaction;
	private long transactionTimestamp;
	private String operation;
	private String tableName;
	private int id;
	private String clientName;
	private String phoneNumber;
	private String areaCode;

	public LockEntity() {
		transactionNumber = -1;
		isTransaction = false;
		transactionTimestamp = -1;
		operation = null;
		tableName = null;
		id = 0;
		clientName = null;
		phoneNumber = null;
		areaCode = null;
	}

	public LockEntity(long _transactionNumber, boolean _isTransaction,
			long _transactionTimestamp, String _operation, String _tableName,
			int _id, String _clientName, String _phoneNumber, String _areaCode) {
		transactionNumber = _transactionNumber;
		isTransaction = _isTransaction;
		transactionTimestamp = _transactionTimestamp;
		operation = _operation;
		tableName = _tableName;
		id = _id;
		clientName = _clientName;
		phoneNumber = _phoneNumber;
		areaCode = _areaCode;
	}

	public LockEntity(LockEntity l) {
		transactionNumber = l.transactionNumber;
		isTransaction = l.isTransaction;
		transactionTimestamp = l.transactionTimestamp;
		operation = l.operation;
		tableName = l.tableName;
		id = l.id;
		clientName = l.clientName;
		phoneNumber = l.phoneNumber;
		areaCode = l.areaCode;
	}

	public long getTransactionNumber() {
		return transactionNumber;
	}

	public void setTransactionNumber(long transactionNumber) {
		this.transactionNumber = transactionNumber;
	}

	public boolean isTransaction() {
		return isTransaction;
	}

	public void setTransaction(boolean isTransaction) {
		this.isTransaction = isTransaction;
	}

	public long getTransactionTimestamp() {
		return transactionTimestamp;
	}

	public void setTransactionTimestamp(long transactionTimestamp) {
		this.transactionTimestamp = transactionTimestamp;
	}

	public String getOperation() {
		return operation;
	}

	public void setOperation(String operation) {
		this.operation = operation;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
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

	public String getAreaCode() {
		return areaCode;
	}

	public void setAreaCode(String areaCode) {
		this.areaCode = areaCode;
	}

	public boolean isConflictingEntity(LockEntity object) {
		if (this.operation.equalsIgnoreCase(Constant.commitCommand)
				|| object.operation.equalsIgnoreCase(Constant.commitCommand)
				|| this.operation.equalsIgnoreCase(Constant.abortCommand)
				|| object.operation.equalsIgnoreCase(Constant.abortCommand)) {
			return false;
		} else {
			if (this.tableName.equals(object.getTableName())) {
				if (this.operation.equalsIgnoreCase(Constant.deleteCommand)
						|| object.operation
								.equalsIgnoreCase(Constant.deleteCommand)) {
					return true;
				} else if (this.operation
						.equalsIgnoreCase(Constant.insertCommand)
						|| object.operation
								.equalsIgnoreCase(Constant.insertCommand)) {
					if (this.id == object.getId()
							|| (this.areaCode != null
									&& object.getAreaCode() != null && this.areaCode
										.equals(object.getAreaCode()))) {
						return true;
					} else {
						return false;
					}
				} else {
					return false;
				}
			} else {
				return false;
			}
		}
	}

	@Override
	public String toString() {
		String argument = " ";

		if (operation.equalsIgnoreCase(Constant.insertCommand)) {
			argument += tableName + " " + "(" + id + ", " + clientName + ", "
					+ phoneNumber + ")";
		} else if (operation.equalsIgnoreCase(Constant.retrieveByIdCommand)) {
			argument += tableName + " " + Integer.toString(id);
		} else if (operation
				.equalsIgnoreCase(Constant.retrieveByAreaCodeCommand)
				|| operation
						.equalsIgnoreCase(Constant.groupByAreaCodeCountCommand)) {
			argument += tableName + " " + areaCode;
		} else if (operation.equalsIgnoreCase(Constant.deleteCommand)) {
			argument += tableName;
		}

		return (isTransaction ? "T: " : "P: ") + transactionNumber + " -> ["
				+ operation + argument + "]";
	}
}
