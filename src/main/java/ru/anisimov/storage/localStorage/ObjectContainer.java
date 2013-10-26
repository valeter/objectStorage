package ru.anisimov.storage.localStorage;

import ru.anisimov.storage.io.FileReaderWriter;

import java.io.File;
import java.io.IOException;

/**
* @author Ivan Anisimov (ivananisimov2010@gmail.com)
*/
public class ObjectContainer {
	private static final long FIRST_POINTER_POSITION = 0;
	private static final long OBJECTS_START_POSITION = 1;

	private static final long END_POINTER = -1;

	private int number;
	private String fileName;
	private long lastByte;

	public ObjectContainer(String fileName, int number, boolean createNew) throws IOException {
		this.fileName = fileName;
		this.number = number;

		if (createNew) {
			new File(this.fileName).createNewFile();
			lastByte = OBJECTS_START_POSITION;
			try (FileReaderWriter out = FileReaderWriter.openForWriting(fileName)) {
				out.writeLong(FIRST_POINTER_POSITION, END_POINTER);
			}
		}
	}

	public static int getNeededSpace(byte[] bytes) {
		return bytes.length + 1;
	}

	public int getNumber() {
		return number;
	}

	public long getLastByte() {
		return lastByte;
	}

	public void removeBytes(long position, int size) throws  IOException {
		try (FileReaderWriter rw = FileReaderWriter.openForReadingWriting(fileName)) {
			long nextPointer = rw.readLong(position + size);
			rw.writeLong(position - 1, nextPointer);
		}
	}

	public void writeBytes(byte[] bytes) throws IOException {
		long nextLastByte = lastByte + getNeededSpace(bytes);
		try (FileReaderWriter out = FileReaderWriter.openForWriting(fileName)) {
			out.writeLong(lastByte - 1, nextLastByte);
			out.writeBytes(lastByte, bytes);
			out.writeLong(nextLastByte - 1, END_POINTER);
		}
		lastByte = nextLastByte;
	}

	public byte[] getBytes(long position, int size) throws IOException {
		try (FileReaderWriter in = FileReaderWriter.openForReading(fileName)) {
			return in.readByte(position, size);
		}
	}

	public long getSize() {
		return new File(fileName).length();
	}

	@Override
	public String toString() {
		return "[Object container, file: " + fileName + " " + getSize() +  "]";
	}
}
