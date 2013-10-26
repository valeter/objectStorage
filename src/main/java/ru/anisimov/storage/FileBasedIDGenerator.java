package ru.anisimov.storage;

import ru.anisimov.storage.commons.TypeSizes;
import ru.anisimov.storage.exceptions.IDGeneratorException;
import ru.anisimov.storage.io.FileReaderWriter;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

/**
 * @author Ivan Anisimov (ivananisimov2010@gmail.com)
 */
class FileBasedIDGenerator implements IDGenerator, Serializable {
	private static final long serialVersionUID = -8681731364222934648L;

	private static final long COUNTER_FLAG_POSITION = 0;
	private static final long COUNTER_POSITION = COUNTER_FLAG_POSITION + 1;
	private static final long FREE_ID_COUNT_POSITION = COUNTER_POSITION + TypeSizes.BYTES_IN_LONG;
	private static final long FREE_ID_POSITION = FREE_ID_COUNT_POSITION + TypeSizes.BYTES_IN_LONG;

	private final long MIN_ID;
	private final long MAX_ID;

	private boolean counterActive;

	private String fileName;

	public FileBasedIDGenerator(String fileName, boolean newGenerator) throws IOException {
		this(fileName, newGenerator, Long.MIN_VALUE, Long.MAX_VALUE);
	}

	FileBasedIDGenerator(String fileName, boolean newGenerator, long MIN_ID, long MAX_ID) throws IOException {
		this.MIN_ID = MIN_ID;
		this.MAX_ID = MAX_ID;
		this.fileName = fileName;
		if (newGenerator) {
			new File(fileName).createNewFile();
			FileReaderWriter.writeBytes(fileName, 0, (byte) 1);
			FileReaderWriter.writeLong(fileName, 1, this.MIN_ID);
		}
		counterActive = FileReaderWriter.readByte(fileName, COUNTER_FLAG_POSITION) == 1;
	}

	@Override
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
		long counter = FileReaderWriter.readLong(fileName, COUNTER_POSITION);
		if (counter >= MAX_ID) {
			FileReaderWriter.writeBytes(fileName, COUNTER_FLAG_POSITION, (byte)-1);
			counterActive = false;
		} else {
			FileReaderWriter.writeLong(fileName, COUNTER_POSITION, counter + 1);
		}
		return counter;
	}

	private long pollLastFreeID() throws IOException, IDGeneratorException {
		long freeIdCount = FileReaderWriter.readLong(fileName, FREE_ID_COUNT_POSITION);
		if (freeIdCount == 0) {
			throw new IDGeneratorException("No more free IDs");
		}
		long lastFreeIDPosition = FREE_ID_POSITION + ((freeIdCount - 1) * TypeSizes.BYTES_IN_LONG);
		long freeID = FileReaderWriter.readLong(fileName, lastFreeIDPosition);
		FileReaderWriter.writeLong(fileName, FREE_ID_COUNT_POSITION, freeIdCount - 1);
		return freeID;
	}

	@Override
	public void addFreeID(long ID) throws IDGeneratorException {
		try {
			long freeIdCount = FileReaderWriter.readLong(fileName, FREE_ID_COUNT_POSITION);
			long positionAfterLastFreeID = FREE_ID_POSITION + ((freeIdCount) * TypeSizes.BYTES_IN_LONG);
			FileReaderWriter.writeLong(fileName, positionAfterLastFreeID, ID);
			FileReaderWriter.writeLong(fileName, FREE_ID_COUNT_POSITION, freeIdCount + 1);
		} catch (IOException e) {
			throw new IDGeneratorException(e);
		}
	}
}
