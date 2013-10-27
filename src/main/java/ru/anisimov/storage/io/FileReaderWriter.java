package ru.anisimov.storage.io;

import ru.anisimov.storage.commons.TypeSizes;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author Ivan Anisimov (ivananisimov2010@gmail.com)
 */
public abstract class FileReaderWriter implements AutoCloseable {
	private final String fileName;
	private FileChannel channel;

	private FileReaderWriter(String fileName) {
		this.fileName = fileName;
	}

	public static FileReaderWriter openForReading(String fileName) throws IOException {
		return new FileReaderWriter(fileName) {
			@Override
			protected FileChannel getChannel(String fileName) throws IOException {
				return new FileInputStream(this.getFileName()).getChannel();
			}
		}.prepare();
	}

	public static FileReaderWriter openForWriting(String fileName) throws IOException {
		return new FileReaderWriter(fileName) {
			@Override
			protected FileChannel getChannel(String fileName) throws IOException {
				return new RandomAccessFile(this.getFileName(), "rw").getChannel();
			}
		}.prepare();
	}

	public static FileReaderWriter openForReadingWriting(String fileName) throws IOException {
		return new FileReaderWriter(fileName) {
			@Override
			protected FileChannel getChannel(String fileName) throws IOException {
				return new RandomAccessFile(this.getFileName(), "rw").getChannel();
			}
		}.prepare();
	}

	protected FileReaderWriter prepare() throws IOException {
		try {
			this.channel = getChannel(fileName);
		} catch (IOException e) {
			throw e;
		}
		return this;
	}

	protected abstract FileChannel getChannel(String fileName) throws IOException;

	protected String getFileName() {
		return fileName;
	}

	public byte[] readByte(long position, int count) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(count);
		channel.read(buffer, position);
		return buffer.array();
	}

	public byte readByte(long position) throws IOException {
		byte[] result = readByte(position, 1);
		return result[0];
	}

	public long readLong(long position) throws IOException {
		byte[] bytes = readByte(position, TypeSizes.BYTES_IN_LONG);
		return ByteBuffer.wrap(bytes).getLong();
	}

	public int readInt(long position) throws IOException {
		byte[] bytes = readByte(position, TypeSizes.BYTES_IN_INT);
		return ByteBuffer.wrap(bytes).getInt();
	}

	public void writeBytes(long position, byte... bytes) throws IOException {
		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		channel.position(position);
		channel.write(buffer);
	}

	public void writeLong(long position, long... numbers) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(TypeSizes.BYTES_IN_LONG * numbers.length);
		for (long number: numbers) {
			buffer.putLong(number);
		}
		writeBytes(position, buffer.array());
	}

	public void writeInt(long position, int... numbers) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(TypeSizes.BYTES_IN_INT * numbers.length);
		for (int number: numbers) {
			buffer.putInt(number);
		}
		writeBytes(position, buffer.array());
	}

	@Override
	public void close() throws IOException {
		if (channel != null)
			channel.close();
	}
}
