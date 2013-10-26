package ru.anisimov.storage.localStorage;

import java.util.Arrays;

/**
 * @author Ivan Anisimov (ivananisimov2010@gmail.com)
 */
class ObjectAddress {
	public static final ObjectAddress EMPTY_ADDRESS = new ObjectAddress(-1, -1, -1);

	private int fileNumber;
	private long filePosition;
	private int objectSize;

	public ObjectAddress(int fileNumber, long filePosition, int objectSize) {
		this.fileNumber = fileNumber;
		this.filePosition = filePosition;
		this.objectSize = objectSize;
	}

	public int getFileNumber() {
		return fileNumber;
	}

	public long getFilePosition() {
		return filePosition;
	}

	public int getObjectSize() {
		return objectSize;
	}

	private Object[] keyArray() {
		return new Object[]{fileNumber, filePosition, objectSize};
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(keyArray());
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (!(obj instanceof ObjectAddress)) {
			return false;
		}

		ObjectAddress that = (ObjectAddress) obj;
		return Arrays.equals(this.keyArray(), that.keyArray());
	}

	@Override
	public String toString() {
		return Arrays.toString(this.keyArray());
	}
}
