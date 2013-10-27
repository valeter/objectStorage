package ru.anisimov.storage.localStorage;

import ru.anisimov.storage.commons.TypeSizes;
import ru.anisimov.storage.io.FileReaderWriter;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

/**
 *
 * @author Ivan Anisimov (ivananisimov2010@gmail.com)
 *
 * ObjectContainer represents single storage file with following structure:
 * |records count - 8 bytes| |ObjectRecord1| ... |ObjectRecordN|
 *
 * Object record structure:
 * |removed record flag - 1 byte| |object ID - 8 bytes| |object size - 4 bytes| |object bytes|
 *
*/
class ObjectContainer {
	private static final long RECORDS_COUNT_POSITION = 0;
	private static final long OBJECT_RECORDS_START_POSITION = RECORDS_COUNT_POSITION + TypeSizes.BYTES_IN_LONG;
	private static final int OBJECT_RECORD_HEADER_SIZE = 1 + TypeSizes.BYTES_IN_LONG + TypeSizes.BYTES_IN_INT;

	private int number;
	private String fileName;
	private long lastByte;
	private long lastRecord;
	private long recordsCount;

	public ObjectContainer(String fileName, int number, boolean createNew) throws IOException {
		this.fileName = fileName;
		this.number = number;
		if (createNew) {
			new File(this.fileName).createNewFile();
			lastByte = OBJECT_RECORDS_START_POSITION;
			lastRecord = -1;
			try (FileReaderWriter out = FileReaderWriter.openForWriting(fileName)) {
				setRecordsCount(out, 0);
			}
		}
		this.recordsCount = parseRecordsCount();
	}

	public static int getNeededSpace(byte[] bytes) {
		return bytes.length + OBJECT_RECORD_HEADER_SIZE;
	}

	public int getNumber() {
		return number;
	}

	public void removeBytes(long position) throws  IOException {
		try (FileReaderWriter out = FileReaderWriter.openForWriting(fileName)) {
			new ObjectRecord(position).remove(out);
			setRecordsCount(out, --recordsCount);
		}
	}

	protected long parseRecordsCount() throws IOException {
		try (FileReaderWriter in = FileReaderWriter.openForReading(fileName)) {
			return in.readLong(RECORDS_COUNT_POSITION);
		}
	}

	private void setRecordsCount(FileReaderWriter out, long count) throws IOException {
		out.writeLong(RECORDS_COUNT_POSITION, count);
	}

	public ObjectAddress writeBytes(int ID, byte[] bytes) throws IOException {
		try (FileReaderWriter out = FileReaderWriter.openForWriting(fileName)) {
			long nextLastByte = lastByte + getNeededSpace(bytes);
			new ObjectRecord(lastByte).write(out, new RecordData(ID, bytes.length, bytes));
			setRecordsCount(out, ++recordsCount);
			lastRecord = lastByte;
			lastByte = nextLastByte;
			return new ObjectAddress(getNumber(), lastRecord);
		}
	}

	public byte[] getBytes(long position) throws IOException {
		try (FileReaderWriter in = FileReaderWriter.openForReading(fileName)) {
			ObjectRecord record = new ObjectRecord(position);
			if (record.isRemoved(in)) {
				return null;
			}
			return record.parseAll(in).getObject();
		}
	}

	public long getSize() {
		return new File(fileName).length();
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

		public RecordData parseHeader(FileReaderWriter in) throws IOException {
			long ID = in.readLong(position + OBJECT_ID_OFFSET);
			int size = in.readInt(position + OBJECT_SIZE_OFFSET);
			return new RecordData(ID, size, null);
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

		public long getPosition() {
			return position;
		}
	}

	public static class RecordData {
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

		@Override
		public String toString() {
			return "[" + ID + " " + size + " " + Arrays.toString(object) + "]";
		}
	}
}
