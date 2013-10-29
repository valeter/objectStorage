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
 * |pointer1 - 8 bytes| ... |pointerN - 8 bytes| |end of file position - 8 bytes| |cell1 - CELL_SIZE bytes| ... |cellN - CELL_SIZE bytes|
 *
 */
public class FileBasedIndex {
	private static final int ESTIMATED_HASH_TABLE_SIZE = 10_000;

	private static final long FIRST_POINTER_POSITION = 0;
	private static final int CELL_SIZE = 3 * TypeSizes.BYTES_IN_LONG + TypeSizes.BYTES_IN_INT;
	private static final long END_POINTER = -1;

	private final int HASH_TABLE_SIZE;
	private final long END_OF_FILE_POSITION;
	private final long FIRST_CELL_POSITION;

	private String fileName;

	public FileBasedIndex(String fileName, boolean newIndex) throws IOException {
		this(fileName, newIndex, ESTIMATED_HASH_TABLE_SIZE);
	}

	FileBasedIndex(String fileName, boolean newIndex, int HASH_TABLE_SIZE) throws IOException {
		this.HASH_TABLE_SIZE = HASH_TABLE_SIZE;
		this.END_OF_FILE_POSITION = FIRST_POINTER_POSITION + (this.HASH_TABLE_SIZE * TypeSizes.BYTES_IN_LONG);
		this.FIRST_CELL_POSITION = this.END_OF_FILE_POSITION + TypeSizes.BYTES_IN_LONG;

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
				out.writeLong(END_OF_FILE_POSITION, FIRST_CELL_POSITION);
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
		long cellPointer = getPointerAddress(ID);

		while (cellPointer != END_POINTER) {
			CellData data = new ObjectAddressCell(cellPointer).parse(in);
			if (data.getID() == ID) {
				return new ObjectAddress(data.getFileNumber(), data.getFilePosition());
			}

			cellPointer = data.getNextPointer();
		}
		return ObjectAddress.EMPTY_ADDRESS;
	}

	private long getPointerAddress(long ID) {
		long hash = Math.abs(ID) % HASH_TABLE_SIZE;
		return FIRST_POINTER_POSITION + (hash * TypeSizes.BYTES_IN_LONG);
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
		long cellPointer = getPointerAddress(ID);
		long prevPointer = -1;
		boolean found = false;
		CellData data = null;
		while (cellPointer != END_POINTER) {
			data = new ObjectAddressCell(cellPointer).parse(rw);
			if (data.getID() == ID) {
				found = true;
				break;
			}

			prevPointer = cellPointer;
			cellPointer = data.getNextPointer();
		}
		if (found) {
			if (prevPointer != -1) {
				new ObjectAddressCell(prevPointer).writeNextPointer(rw, data.getNextPointer());
			} else {
				new ObjectAddressCell(cellPointer).writeNextPointer(rw, END_POINTER);
			}
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
		long cellPointer = getPointerAddress(ID);
		long prevPointer = -1;
		boolean found = false;
		while (cellPointer != END_POINTER) {
			CellData data = new ObjectAddressCell(cellPointer).parse(rw);
			if (data.getID() == ID) {
				found = true;
				break;
			}

			prevPointer = cellPointer;
			cellPointer = data.getNextPointer();
		}
		long endOfFile = rw.readLong(END_OF_FILE_POSITION);
		long nextPointer = (found) ? cellPointer : endOfFile;
		if (!found) {
			rw.writeLong(END_OF_FILE_POSITION, endOfFile + CELL_SIZE);
			new ObjectAddressCell(nextPointer).writeNextPointer(rw, END_POINTER);
		}
		new ObjectAddressCell(nextPointer).writeIDAndAddress(rw, ID, address);
		if (prevPointer != -1) {
			new ObjectAddressCell(prevPointer).writeNextPointer(rw, nextPointer);
		}
	}

	private class ObjectAddressCell {
		private static final int CELL_OFFSET_NEXT_POINTER = 0;
		private static final int CELL_OFFSET_ID = CELL_OFFSET_NEXT_POINTER + TypeSizes.BYTES_IN_LONG;
		private static final int CELL_OFFSET_FILE_NUM = CELL_OFFSET_ID + TypeSizes.BYTES_IN_LONG;
		private static final int CELL_OFFSET_FILE_POSITION = CELL_OFFSET_FILE_NUM + TypeSizes.BYTES_IN_INT;

		private long position;

		public ObjectAddressCell(long position) {
			this.position = position;
		}

		public CellData parse(FileReaderWriter in) throws IOException {
			long ID = in.readLong(position + CELL_OFFSET_ID);
			int fileNumber = in.readInt(position + CELL_OFFSET_FILE_NUM);
			long filePosition = in.readLong(position + CELL_OFFSET_FILE_POSITION);
			long nextPointer = in.readLong(position + CELL_OFFSET_NEXT_POINTER);
			return new CellData(ID, fileNumber, filePosition, nextPointer);
		}

		public ObjectAddressCell writeIDAndAddress(FileReaderWriter out, long ID, ObjectAddress address) throws IOException {
			out.writeLong(position + CELL_OFFSET_ID, ID);
			out.writeInt(position + CELL_OFFSET_FILE_NUM, address.getFileNumber());
			out.writeLong(position + CELL_OFFSET_FILE_POSITION, address.getFilePosition());
			return this;
		}

		public ObjectAddressCell writeNextPointer(FileReaderWriter out, long pointer) throws IOException {
			out.writeLong(position + CELL_OFFSET_NEXT_POINTER, pointer);
			return this;
		}
	}

	private static class CellData {
		private long ID;
		private int fileNumber;
		private long filePosition;
		private long nextPointer;

		public CellData(long ID, int fileNumber, long filePosition, long nextPointer) {
			this.ID = ID;
			this.fileNumber = fileNumber;
			this.filePosition = filePosition;
			this.nextPointer = nextPointer;
		}

		public long getID() {
			return ID;
		}

		public int getFileNumber() {
			return fileNumber;
		}

		public long getFilePosition() {
			return filePosition;
		}

		public long getNextPointer() {
			return nextPointer;
		}
	}
}
