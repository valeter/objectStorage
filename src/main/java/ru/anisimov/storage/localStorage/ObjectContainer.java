package ru.anisimov.storage.localStorage;

import ru.anisimov.storage.commons.TypeSizes;
import ru.anisimov.storage.io.FileReaderWriter;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author Ivan Anisimov (ivananisimov2010@gmail.com)
 *
 * ObjectContainer represents single storage file with following structure:
 * |records count - 4 bytes| |last byte pointer - 8 bytes| |ObjectRecord1| ... |ObjectRecordN|
 *
 * Object record structure:
 * |removed record flag - 1 byte| |object ID - 8 bytes| |object size - 4 bytes| |object bytes|
 *
*/
class ObjectContainer {
	private static final long RECORDS_COUNT_POSITION = 0;
	private static final long LAST_BYTE_POINTER_POSITION = RECORDS_COUNT_POSITION + TypeSizes.BYTES_IN_INT;
	private static final long OBJECT_RECORDS_START_POSITION = LAST_BYTE_POINTER_POSITION + TypeSizes.BYTES_IN_LONG;
	private static final int OBJECT_RECORD_HEADER_SIZE = 1 + TypeSizes.BYTES_IN_LONG + TypeSizes.BYTES_IN_INT;

	private String fileName;
	private int number;
	private long lastByte;
	private int recordsCount;

	public ObjectContainer(FileReaderWriter rw, String fileName, int number, boolean createNew) throws IOException {
		this.fileName = fileName;
		this.number = number;
		if (createNew) {
			File file = new File(this.fileName);
			if (file.exists()) {
				file.delete();
			}
			file.createNewFile();
			rw.writeInt(RECORDS_COUNT_POSITION, 0);
			rw.writeLong(LAST_BYTE_POINTER_POSITION, OBJECT_RECORDS_START_POSITION);
		}
		parseFields(rw);
	}

	public static int getNeededSpace(byte[] bytes) {
		return bytes.length + OBJECT_RECORD_HEADER_SIZE;
	}

	private void parseFields(FileReaderWriter in) throws IOException {
		recordsCount = in.readInt(RECORDS_COUNT_POSITION);
		lastByte = in.readLong(LAST_BYTE_POINTER_POSITION);
	}

	public int getNumber() {
		return number;
	}

	protected long getRecordsCount() throws IOException {
		return recordsCount;//in.readLong(RECORDS_COUNT_POSITION);
	}

	public void removeBytes(FileReaderWriter out, long position) throws  IOException {
		removeBytes(out, new long[] {position});
	}

	public void removeBytes(FileReaderWriter out, long[] positions) throws  IOException {
		for (int i = 0; i < positions.length; i++) {
			if (positions[i] < 0) {
				continue;
			}
			new ObjectRecord(positions[i]).remove(out);
			recordsCount--;
		}
		out.writeInt(RECORDS_COUNT_POSITION, recordsCount);
	}

	public ObjectAddress writeBytes(FileReaderWriter out, long ID, byte[] bytes) throws IOException {
		return writeBytes(out, new long[] {ID}, new byte[][]{bytes})[0];
	}

	public ObjectAddress[] writeBytes(FileReaderWriter out, long[] ID, byte[][] bytes) throws IOException {
		return writeBytes(out, ID, bytes, 0, ID.length);
	}

	public ObjectAddress[] writeBytes(FileReaderWriter out, long[] ID, byte[][] bytes, int from, int count) throws IOException {
		int objectsCount = count;
		ObjectAddress[] result = new ObjectAddress[objectsCount];
		for (int i = from; i < from + count; i++) {
			result[i - from] = writeObject(out, ID[i], bytes[i]);
			recordsCount++;
		}
		out.writeInt(RECORDS_COUNT_POSITION, recordsCount);
		return result;
	}

	private ObjectAddress writeObject(FileReaderWriter out, long ID, byte[] bytes) throws IOException {
		long nextLastByte = lastByte + getNeededSpace(bytes);
		new ObjectRecord(lastByte).write(out, new RecordData(ID, bytes.length, bytes));
		long lastRecord = lastByte;
		lastByte = nextLastByte;
		return new ObjectAddress(getNumber(), lastRecord);
	}

	public RecordData getData(FileReaderWriter in, long position) throws IOException {
		return getData(in, new long[] {position})[0];
	}

	public RecordData[] getData(FileReaderWriter in, long[] positions) throws IOException {
		RecordData[] result = new RecordData[positions.length];
		for (int i = 0; i < positions.length; i++) {
			if (positions[i] < 0) {
				result[i] = null;
				continue;
			}
			ObjectRecord record = new ObjectRecord(positions[i]);
			if (record.isRemoved(in)) {
				result[i] = null;
				continue;
			}
			result[i] = record.parseAll(in);
		}
		return result;
	}

	public long getSize() {
		return lastByte;
	}

	public List<ObjectAddress> getRecordsAddresses(FileReaderWriter in) throws IOException {
		long recordsCount = getRecordsCount();
		List<ObjectAddress> result = new LinkedList<>();

		long pointer = OBJECT_RECORDS_START_POSITION;
		while (recordsCount > 0 && pointer <= getSize()) {
			ObjectRecord record = new ObjectRecord(pointer);
			if (!record.isRemoved(in)) {
				RecordData data = record.parseAll(in);
				ObjectAddress address = new ObjectAddress(number, pointer);

				if (data.getObject() != null && data.getSize() == data.getObject().length) {
					result.add(address);
					recordsCount--;
				}
			}
			long nextPointer = record.getNextRecord(in);
			if (nextPointer <= pointer) {
				break;
			}
			pointer = nextPointer;
		}

		return result;
	}

	@Override
	public String toString() {
		return "[Object container, file: " + fileName + " " + getSize() +  "]";
	}

	protected class ObjectRecord {
		private static final int REMOVE_FLAG_OFFSET = 0;
		private static final int OBJECT_ID_OFFSET = REMOVE_FLAG_OFFSET + 1;
		private static final int OBJECT_SIZE_OFFSET = OBJECT_ID_OFFSET + TypeSizes.BYTES_IN_LONG;
		private static final int OBJECT_OFFSET = OBJECT_SIZE_OFFSET + TypeSizes.BYTES_IN_INT;

		private static final byte ACTIVE = 1;
		private static final byte REMOVED = -1;

		private long position;

		public ObjectRecord(long position) {
			this.position = position;
		}

		public long getNextRecord(FileReaderWriter in) throws IOException {
			int objectSize = in.readInt(position + OBJECT_SIZE_OFFSET);
			return position + OBJECT_RECORD_HEADER_SIZE + objectSize;
		}

		public void remove(FileReaderWriter out) throws IOException {
			out.writeBytes(position + REMOVE_FLAG_OFFSET, REMOVED);
		}

		public boolean isRemoved(FileReaderWriter in) throws IOException {
			byte removeFlag = in.readByte(position + REMOVE_FLAG_OFFSET);
			return removeFlag == REMOVED;
		}

		public RecordData parseAll(FileReaderWriter in) throws IOException {
			long ID = in.readLong(position + OBJECT_ID_OFFSET);
			int size = in.readInt(position + OBJECT_SIZE_OFFSET);
			byte[] object = in.readByte(position + OBJECT_OFFSET, size);
			return new RecordData(ID, size, object);
		}

		public void write(FileReaderWriter out, RecordData data) throws IOException {
			out.writeBytes(position + REMOVE_FLAG_OFFSET, ObjectRecord.ACTIVE);
			out.writeLong(position + OBJECT_ID_OFFSET, data.getID());
			out.writeInt(position + OBJECT_SIZE_OFFSET, data.getSize());
			out.writeBytes(position + OBJECT_OFFSET, data.getObject());
		}
	}
}
