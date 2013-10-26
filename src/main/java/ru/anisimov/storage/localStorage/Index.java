package ru.anisimov.storage.localStorage;

import ru.anisimov.storage.commons.TypeSizes;
import ru.anisimov.storage.io.FileReaderWriter;

import java.io.File;
import java.io.IOException;

/**
 * @author Ivan Anisimov (ivananisimov2010@gmail.com)
 */
class Index {
	private static final int ESTIMATED_HASH_TABLE_SIZE = 1_000_000;

	private static final long FIRST_POINTER_POSITION = 0;
	private static final int CELL_SIZE = 5 * TypeSizes.BYTES_IN_LONG;
	private static final int CELL_OFFSET_ID = 0 * TypeSizes.BYTES_IN_LONG;
	private static final int CELL_OFFSET_FILE_NUM = 1 * TypeSizes.BYTES_IN_LONG;
	private static final int CELL_OFFSET_FILE_POSITION = 2 * TypeSizes.BYTES_IN_LONG;
	private static final int CELL_OFFSET_OBJECT_SIZE = 3 * TypeSizes.BYTES_IN_LONG;
	private static final int CELL_OFFSET_NEXT_POINTER = 4 * TypeSizes.BYTES_IN_LONG;
	private static final long END_POINTER = -1;

	private final int HASH_TABLE_SIZE;
	private final long CELL_COUNT_POSITION;
	private final long FIRST_CELL_POSITION;

	private String fileName;

	public Index(String fileName, boolean newIndex) throws IOException {
		this(fileName, newIndex, ESTIMATED_HASH_TABLE_SIZE);
	}

	Index(String fileName, boolean newIndex, int HASH_TABLE_SIZE) throws IOException {
		this.HASH_TABLE_SIZE = HASH_TABLE_SIZE;
		this.CELL_COUNT_POSITION = this.HASH_TABLE_SIZE * TypeSizes.BYTES_IN_LONG;
		this.FIRST_CELL_POSITION = this.CELL_COUNT_POSITION + TypeSizes.BYTES_IN_LONG;

		this.fileName = fileName;
		if (newIndex) {
			new File(fileName).createNewFile();
			try (FileReaderWriter out = FileReaderWriter.openForWriting(this.fileName)) {
				for (int i = 0; i < this.HASH_TABLE_SIZE; i++) {
					out.writeLong(FIRST_POINTER_POSITION + (i * TypeSizes.BYTES_IN_LONG), END_POINTER);
				}
				out.writeLong(CELL_COUNT_POSITION, 0);
			}
		}
	}

	public ObjectAddress getAddress(long ID) throws IOException {
		long pointerAddress = getPointerAddress(ID);

		try (FileReaderWriter in = FileReaderWriter.openForReading(fileName)) {
			long cellAddress = in.readLong(pointerAddress);

			while (cellAddress != END_POINTER) {
				long cellID = in.readLong(cellAddress + CELL_OFFSET_ID);
				if (cellID == ID) {
					long cellFileNum = in.readLong(cellAddress + CELL_OFFSET_FILE_NUM);
					long cellFilePosition = in.readLong(cellAddress + CELL_OFFSET_FILE_POSITION);
					long cellObjectSize = in.readLong(cellAddress + CELL_OFFSET_OBJECT_SIZE);

					return new ObjectAddress(cellFileNum, cellFilePosition, cellObjectSize);
				}

				cellAddress = in.readLong(cellAddress + CELL_OFFSET_NEXT_POINTER);
			}
		}
		return ObjectAddress.EMPTY_ADDRESS;
	}

	private long getPointerAddress(long ID) {
		long hash = Math.abs(ID % HASH_TABLE_SIZE);
		return hash * TypeSizes.BYTES_IN_LONG;
	}

	public void putAddress(long ID, ObjectAddress address) throws IOException {
		long pointerAddress = getPointerAddress(ID);

		try (FileReaderWriter rw = FileReaderWriter.openForReadingWriting(fileName)) {
			long cellAddress = rw.readLong(pointerAddress);

			while (cellAddress != END_POINTER) {
				long cellID = rw.readLong(cellAddress + CELL_OFFSET_ID);
				if (cellID == ID) {
					writeObjectAddress(rw, cellAddress, address);
					return;
				}

				long tempAddress = rw.readLong(cellAddress + CELL_OFFSET_NEXT_POINTER);
				if (tempAddress != END_POINTER) {
					cellAddress = tempAddress;
				} else {
					break;
				}
			}

			if (cellAddress == END_POINTER) {
				cellAddress = pointerAddress;
			}

			long cellCount = rw.readLong(CELL_COUNT_POSITION);
			long nextAddress = FIRST_CELL_POSITION + cellCount * CELL_SIZE;
			rw.writeLong(cellAddress, nextAddress);
			rw.writeLong(nextAddress + CELL_OFFSET_ID, ID);
			writeObjectAddress(rw, nextAddress, address);
			rw.writeLong(nextAddress + CELL_OFFSET_NEXT_POINTER, END_POINTER);
			rw.writeLong(CELL_COUNT_POSITION, cellCount + 1);
		}
	}

	private void writeObjectAddress(FileReaderWriter out, long position, ObjectAddress address) throws IOException {
		out.writeLong(position + CELL_OFFSET_FILE_NUM, address.getFileNumber());
		out.writeLong(position + CELL_OFFSET_FILE_POSITION, address.getFilePosition());
		out.writeLong(position + CELL_OFFSET_OBJECT_SIZE, address.getObjectSize());
	}
}
