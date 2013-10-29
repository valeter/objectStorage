package ru.anisimov.storage.localStorage;

import ru.anisimov.storage.commons.TypeSizes;
import ru.anisimov.storage.exceptions.IndexException;
import ru.anisimov.storage.io.FileReaderWriter;

import java.io.File;
import java.io.IOException;

/**
 * @author Ivan Anisimov (ivananisimov2010@gmail.com)
 *
 * Represents single index hash table file with following structure:
 *
 *
 */
public class FileBasedIndex {
	private static final int ESTIMATED_HASH_TABLE_SIZE = 10_000;

	private static final long FIRST_POINTER_POSITION = 0;
	private static final int CELL_SIZE = 5 * TypeSizes.BYTES_IN_LONG;
	private static final int CELL_OFFSET_ID = 0 * TypeSizes.BYTES_IN_LONG;
	private static final int CELL_OFFSET_FILE_NUM = 1 * TypeSizes.BYTES_IN_LONG;
	private static final int CELL_OFFSET_FILE_POSITION = 2 * TypeSizes.BYTES_IN_LONG;
	private static final int CELL_OFFSET_NEXT_POINTER = 3 * TypeSizes.BYTES_IN_LONG;
	private static final long END_POINTER = -1;

	private final int HASH_TABLE_SIZE;
	private final long CELL_COUNT_POSITION;
	private final long FIRST_CELL_POSITION;

	private String fileName;

	public FileBasedIndex(String fileName, boolean newIndex) throws IOException {
		this(fileName, newIndex, ESTIMATED_HASH_TABLE_SIZE);
	}

	FileBasedIndex(String fileName, boolean newIndex, int HASH_TABLE_SIZE) throws IOException {
		this.HASH_TABLE_SIZE = HASH_TABLE_SIZE;
		this.CELL_COUNT_POSITION = FIRST_POINTER_POSITION + this.HASH_TABLE_SIZE * TypeSizes.BYTES_IN_LONG;
		this.FIRST_CELL_POSITION = this.CELL_COUNT_POSITION + TypeSizes.BYTES_IN_LONG;

		this.fileName = fileName;
		if (newIndex) {
			File file = new File(this.fileName);
			if (file.exists()) {
				file.delete();
			}
			file.createNewFile();
			try (FileReaderWriter out = FileReaderWriter.openForWriting(this.fileName)) {
				for (int i = 0; i < this.HASH_TABLE_SIZE; i++) {
					out.writeLong(FIRST_POINTER_POSITION + (i * TypeSizes.BYTES_IN_LONG), END_POINTER);
				}
				out.writeLong(CELL_COUNT_POSITION, 0);
			}
		}
	}

	public ObjectAddress getAddress(long ID) throws IndexException {
		return getAddress(new long[] {ID})[0];
	}

	public ObjectAddress[] getAddress(long[] ID) throws IndexException {
		try (FileReaderWriter in = FileReaderWriter.openForReading(fileName)) {
			int resultCount = ID.length;
			ObjectAddress[] result = new ObjectAddress[resultCount];
			for (int i = 0; i < resultCount; i++) {
				result[i] = getAddress(in, ID[i]);
			}
			return result;
		} catch (IOException e) {
			throw new IndexException(e);
		}
	}

	private ObjectAddress getAddress(FileReaderWriter in, long ID) throws IOException {
		long pointerAddress = getPointerAddress(ID);
		long cellAddress = in.readLong(pointerAddress);

		while (cellAddress != END_POINTER) {
			long cellID = in.readLong(cellAddress + CELL_OFFSET_ID);
			if (cellID == ID) {
				int cellFileNum = (int) in.readLong(cellAddress + CELL_OFFSET_FILE_NUM);
				long cellFilePosition = in.readLong(cellAddress + CELL_OFFSET_FILE_POSITION);

				return new ObjectAddress(cellFileNum, cellFilePosition);
			}

			cellAddress = in.readLong(cellAddress + CELL_OFFSET_NEXT_POINTER);
		}
		return ObjectAddress.EMPTY_ADDRESS;
	}

	private long getPointerAddress(long ID) {
		long hash = Math.abs(ID % HASH_TABLE_SIZE);
		return hash * TypeSizes.BYTES_IN_LONG;
	}

	public void removeAddress(long ID) throws IndexException {
		removeAddress(new long[] {ID});
	}

	public void removeAddress(long[] ID) throws IndexException {
		try (FileReaderWriter rw = FileReaderWriter.openForReadingWriting(fileName)) {
			for (int i = 0; i < ID.length; i++) {
				removeAddress(rw, ID[i]);
			}
		} catch (IOException e) {
			throw new IndexException(e);
		}
	}

	private void removeAddress(FileReaderWriter rw, long ID) throws IOException {
		long pointerAddress = getPointerAddress(ID);
		long cellAddress = rw.readLong(pointerAddress);
		long prevAddress = pointerAddress;
		boolean found = false;
		while (cellAddress != END_POINTER) {
			long cellID = rw.readLong(cellAddress + CELL_OFFSET_ID);
			if (cellID == ID) {
				found = true;
				break;
			}

			prevAddress = cellAddress;
			cellAddress = rw.readLong(cellAddress + CELL_OFFSET_NEXT_POINTER);
		}

		if (found) {
			long nextAddress = rw.readLong(cellAddress + CELL_OFFSET_NEXT_POINTER);
			rw.writeLong(prevAddress, nextAddress);
		}
	}

	public void putAddress(long ID, ObjectAddress address) throws IndexException {
		putAddress(new long[] {ID}, new ObjectAddress[] {address});
	}

	public void putAddress(long[] ID, ObjectAddress[] address) throws IndexException {
		try (FileReaderWriter rw = FileReaderWriter.openForReadingWriting(fileName)) {
			for (int i = 0; i < ID.length; i++) {
				putAddress(rw, ID[i], address[i]);
			}
		} catch (IOException e) {
			throw new IndexException(e);
		}
	}

	private void putAddress(FileReaderWriter rw, long ID, ObjectAddress address) throws IOException {
		long pointerAddress = getPointerAddress(ID);
		long cellAddress = rw.readLong(pointerAddress);
		long prevAddress = pointerAddress;
		while (cellAddress != END_POINTER) {
			long cellID = rw.readLong(cellAddress + CELL_OFFSET_ID);
			if (cellID == ID) {
				writeObjectAddress(rw, cellAddress, address);
				return;
			}

			cellAddress = rw.readLong(cellAddress + CELL_OFFSET_NEXT_POINTER);
			prevAddress = cellAddress;
		}

		long cellCount = rw.readLong(CELL_COUNT_POSITION);
		long nextAddress = FIRST_CELL_POSITION + cellCount * CELL_SIZE;
		rw.writeLong(prevAddress, nextAddress);
		rw.writeLong(nextAddress + CELL_OFFSET_ID, ID);
		writeObjectAddress(rw, nextAddress, address);
		rw.writeLong(nextAddress + CELL_OFFSET_NEXT_POINTER, END_POINTER);
		rw.writeLong(CELL_COUNT_POSITION, cellCount + 1);
	}

	private void writeObjectAddress(FileReaderWriter out, long position, ObjectAddress address) throws IOException {
		out.writeLong(position + CELL_OFFSET_FILE_NUM, address.getFileNumber());
		out.writeLong(position + CELL_OFFSET_FILE_POSITION, address.getFilePosition());
	}
}
