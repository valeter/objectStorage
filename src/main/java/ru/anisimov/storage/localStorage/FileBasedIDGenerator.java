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
 * |optimization counter - 8 bytes| |free id counter - 8 bytes| |free id| ... |free id|
 *
 * Optimization counter at start = Integer.MIN_VALUE. It represents maximal free id.
 * While there's free IDs in list generator poll them. If there's no free IDs in list generator increments counter.
 * If counter reaches max value generator throws IDGeneratorException - there's no more free ID's.
 *
 */
public class FileBasedIDGenerator implements Serializable {
	private static final long serialVersionUID = -8681731364222934648L;

	private static final long MAX_ELEMENTS_NUM = 100_000_000l;

	private static final long COUNTER_POSITION = 0l;
	private static final long FREE_ID_COUNT_POSITION = COUNTER_POSITION + TypeSizes.BYTES_IN_LONG;
	private static final long FREE_ID_POSITION = FREE_ID_COUNT_POSITION + TypeSizes.BYTES_IN_LONG;

	private final long MIN_ID;
	private final long MAX_ID;

	private String fileName;
	private long freeIDCount;

	public FileBasedIDGenerator(String fileName, boolean newGenerator) throws IOException {
		this(fileName, newGenerator, Integer.MIN_VALUE, (long)(Integer.MIN_VALUE) + MAX_ELEMENTS_NUM);
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
				rw.writeLong(COUNTER_POSITION, this.MIN_ID);
				rw.writeLong(FREE_ID_COUNT_POSITION, 0);
			}
			freeIDCount = rw.readLong(FREE_ID_COUNT_POSITION);
		}
	}

	public long generateID() throws IDGeneratorException {
		return generateID(1)[0];
	}

	public long[] generateID(int count) throws IDGeneratorException {
		try (FileReaderWriter rw = FileReaderWriter.openForReadingWriting(fileName)) {
			long[] result = new long[count];
			for (int i = 0; i < count; i++) {
				result[i] = (freeIDCount == 0) ?
									getAndIncrementCounter(rw) :
									pollLastFreeID(rw);
			}
			return result;
		} catch (IOException e) {
			throw  new IDGeneratorException(e);
		}
	}

	private long getAndIncrementCounter(FileReaderWriter rw) throws IDGeneratorException, IOException {
		long counter = rw.readLong(COUNTER_POSITION);
		if (counter > MAX_ID) {
			throw new IDGeneratorException("No more free IDs");
		} else {
			rw.writeLong(COUNTER_POSITION, counter + 1);
		}
		return counter;
	}

	private long pollLastFreeID(FileReaderWriter rw) throws IOException {
		long lastFreeIDPosition = FREE_ID_POSITION + ((freeIDCount - 1) * TypeSizes.BYTES_IN_LONG);
		long freeID = rw.readLong(lastFreeIDPosition);
		rw.writeLong(FREE_ID_COUNT_POSITION, --freeIDCount);
		return freeID;
	}

	public void addFreeID(long ID) throws IDGeneratorException {
		addFreeID(new long[] {ID});
	}

	public void addFreeID(long[] ID) throws IDGeneratorException {
		try (FileReaderWriter rw = FileReaderWriter.openForReadingWriting(fileName)) {
			for (int i = 0; i < ID.length; i++) {
				long positionAfterLastFreeID = FREE_ID_POSITION + ((freeIDCount) * TypeSizes.BYTES_IN_LONG);
				rw.writeLong(positionAfterLastFreeID, ID[i]);
				rw.writeLong(FREE_ID_COUNT_POSITION, ++freeIDCount);
			}
		} catch (IOException e) {
			throw new IDGeneratorException(e);
		}
	}
}
