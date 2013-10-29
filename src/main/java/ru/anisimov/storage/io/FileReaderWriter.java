package ru.anisimov.storage.io;

import ru.anisimov.storage.commons.TypeSizes;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author Ivan Anisimov (ivananisimov2010@gmail.com)
 */
public abstract class FileReaderWriter implements AutoCloseable {
	private FileChannel channel;

	public static FileReaderWriter openForReading(final String fileName) throws IOException {
		return new FileReaderWriter() {
			@Override
			protected FileChannel getChannel() throws IOException {
				return new FileInputStream(fileName).getChannel();
			}
		}.prepare();
	}

	public static FileReaderWriter openForWriting(final String fileName) throws IOException {
		return new FileReaderWriter() {
			@Override
			protected FileChannel getChannel() throws IOException {
				return new RandomAccessFile(fileName, "rw").getChannel();
			}
		}.prepare();
	}

	public static FileReaderWriter openForReadingWriting(final String fileName) throws IOException {
		return new FileReaderWriter() {
			@Override
			protected FileChannel getChannel() throws IOException {
				return new RandomAccessFile(fileName, "rw").getChannel();
			}
		}.prepare();
	}

	protected FileReaderWriter prepare() throws IOException {
		this.channel = getChannel();
		return this;
	}

	protected abstract FileChannel getChannel() throws IOException;

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
		ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, position, bytes.length);
		buffer.put(bytes);
	}

	public void writeLong(long position, long number) throws IOException {
		ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, position, TypeSizes.BYTES_IN_LONG);
		buffer.putLong(number);
	}

	public void writeInt(long position, int number) throws IOException {
		ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, position, TypeSizes.BYTES_IN_INT);
		buffer.putInt(number);
	}


	@Override
	public void close() throws IOException {
		channel.close();
	}
}
