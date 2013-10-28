package ru.anisimov.storage.localStorage;

import ru.anisimov.storage.commons.TypeSizes;
import ru.anisimov.storage.exceptions.IDGeneratorException;
import ru.anisimov.storage.io.FileReaderWriter;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

/**
 * @author Ivan Anisimov (ivananisimov2010@gmail.com)
 *
 * Represents single file with following structure:
 * |optimization counter flag - 1 byte| |optimization counter - 8 bytes| |free id counter - 8 bytes| |free id| ... |free id|
 *
 * Optimization counter at start = Integer.MIN_VALUE. If otimization counter flag is active, this counter represents free id.
 * After id is taken, optimization counter increments. After it reaches Integer.MIN_VALUE + ESTIMATED_ELEMENTS_NUM it's flag sets to inactive state and
 * generator starts to use only free id's from list at the end.
 *
 */
public class FileBasedIDGenerator implements Serializable {
	private static final long serialVersionUID = -8681731364222934648L;

	private static final long ESTIMATED_ELEMENTS_NUM = (long)(Integer.MAX_VALUE) * 2l;

	private static final long COUNTER_FLAG_POSITION = 0;
	private static final long COUNTER_POSITION = COUNTER_FLAG_POSITION + 1;
	private static final long FREE_ID_COUNT_POSITION = COUNTER_POSITION + TypeSizes.BYTES_IN_LONG;
	private static final long FREE_ID_POSITION = FREE_ID_COUNT_POSITION + TypeSizes.BYTES_IN_LONG;

	private static final byte ACTIVE_COUNTER = 1;
	private static final byte INACTIVE_COUNTER = -1;

	private final long MIN_ID;
	private final long MAX_ID;

	private boolean counterActive;

	private String fileName;

	public FileBasedIDGenerator(String fileName, boolean newGenerator) throws IOException {
		this(fileName, newGenerator, Integer.MIN_VALUE, (long)(Integer.MIN_VALUE) + ESTIMATED_ELEMENTS_NUM);
	}

	FileBasedIDGenerator(String fileName, boolean newGenerator, long MIN_ID, long MAX_ID) throws IOException {
		this.MIN_ID = MIN_ID;
		this.MAX_ID = MAX_ID;
		this.fileName = fileName;
		try (FileReaderWriter rw = FileReaderWriter.openForReadingWriting(this.fileName)) {
			if (newGenerator) {
				File file = new File(this.fileName);
				if (file.exists()) {
					file.delete();
				}
				file.createNewFile();
				rw.writeBytes(COUNTER_FLAG_POSITION, ACTIVE_COUNTER);
				rw.writeLong(COUNTER_POSITION, this.MIN_ID);
			}
			counterActive = rw.readByte(COUNTER_FLAG_POSITION) == ACTIVE_COUNTER;
		}
	}

	public long generateID() throws IDGeneratorException {
		try {
			return (counterActive) ?
						   getAndIncrementCounter() :
						   pollLastFreeID();
		} catch (IOException e) {
			throw new IDGeneratorException(e);
		}
	}

	private long getAndIncrementCounter() throws IOException {
		try (FileReaderWriter rw = FileReaderWriter.openForReadingWriting(fileName)) {
			long counter = rw.readLong(COUNTER_POSITION);
			if (counter >= MAX_ID) {
				rw.writeBytes(COUNTER_FLAG_POSITION, INACTIVE_COUNTER);
				counterActive = false;
			} else {
				rw.writeLong(COUNTER_POSITION, counter + 1);
			}
			return counter;
		}
	}

	private long pollLastFreeID() throws IOException, IDGeneratorException {
		try (FileReaderWriter rw = FileReaderWriter.openForReadingWriting(fileName)) {
			long freeIdCount = rw.readLong(FREE_ID_COUNT_POSITION);
			if (freeIdCount == 0) {
				throw new IDGeneratorException("No more free IDs");
			}
			long lastFreeIDPosition = FREE_ID_POSITION + ((freeIdCount - 1) * TypeSizes.BYTES_IN_LONG);
			long freeID = rw.readLong(lastFreeIDPosition);
			rw.writeLong(FREE_ID_COUNT_POSITION, freeIdCount - 1);
			return freeID;
		}
	}

	public void addFreeID(long ID) throws IDGeneratorException {
		try (FileReaderWriter rw = FileReaderWriter.openForReadingWriting(fileName)) {
			addFreeID(rw, ID);
		} catch (IOException e) {
			throw new IDGeneratorException(e);
		}
	}

	public void addFreeID(long[] ID) throws IDGeneratorException {
		try (FileReaderWriter rw = FileReaderWriter.openForReadingWriting(fileName)) {
			for (int i = 0; i < ID.length; i++) {
				addFreeID(rw, ID[i]);
			}
		} catch (IOException e) {
			throw new IDGeneratorException(e);
		}
	}

	private void addFreeID(FileReaderWriter rw, long ID) throws IOException {
		long freeIdCount = rw.readLong(FREE_ID_COUNT_POSITION);
		long positionAfterLastFreeID = FREE_ID_POSITION + ((freeIdCount) * TypeSizes.BYTES_IN_LONG);
		rw.writeLong(positionAfterLastFreeID, ID);
		rw.writeLong(FREE_ID_COUNT_POSITION, freeIdCount + 1);
	}
}