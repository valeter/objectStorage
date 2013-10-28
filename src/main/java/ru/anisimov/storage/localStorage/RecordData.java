package ru.anisimov.storage.localStorage;

import java.util.Arrays;

/**
* @author Ivan Anisimov (ivananisimov2010@gmail.com)
*/
public class RecordData {
	private long ID;
	private int size;
	private byte[] object;

	public RecordData(long ID, int size, byte[] object) {
		this.ID = ID;
		this.size = size;
		this.object = object;
	}

	public byte[] getObject() {
		return object;
	}

	public int getSize() {
		return size;
	}

	public long getID() {
		return ID;
	}

	private Object[] keyArray() {
		return new Object[] {ID, size};
	}

	@Override
	public int hashCode() {
		int prime = 31;
		int result = 17;
		result = prime * result + Arrays.hashCode(keyArray());
		result = prime * result + Arrays.hashCode(object);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (!(obj instanceof RecordData)) {
			return false;
		}

		RecordData that = (RecordData) obj;
		return Arrays.equals(this.keyArray(), that.keyArray()) && Arrays.equals(this.getObject(), that.getObject());
	}

	@Override
	public String toString() {
		return Arrays.toString(this.keyArray());
	}
}
