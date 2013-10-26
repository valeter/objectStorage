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
public class FileReaderWriter {
	private FileReaderWriter() {
		throw new UnsupportedOperationException();
	}

	public static byte[] readByte(String fileName, long position, int count) throws IOException {
		try (FileChannel in = new FileInputStream(fileName).getChannel()) {
			ByteBuffer buffer = ByteBuffer.allocate(count);
			in.read(buffer, position);
			return buffer.array();
		} catch (IOException e) {
			throw e;
		}
	}

	public static byte readByte(String fileName, long position) throws IOException {
		byte[] result = readByte(fileName, position, 1);
		return result[0];
	}

	public static long readLong(String fileName, long position) throws IOException {
		byte[] bytes = readByte(fileName, position, TypeSizes.BYTES_IN_LONG);
		return ByteBuffer.wrap(bytes).getLong();
	}

	public static void writeBytes(String fileName, long position, byte... bytes) throws IOException {
		try (FileChannel out = new RandomAccessFile(fileName, "rw").getChannel()) {
			ByteBuffer buffer = ByteBuffer.wrap(bytes);
			out.position(position);
			out.write(buffer);
		} catch (IOException e) {
			throw e;
		}
	}

	public static void writeLong(String fileName, long position, long... numbers) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(TypeSizes.BYTES_IN_LONG * numbers.length);
		for (long number: numbers) {
			buffer.putLong(number);
		}
		writeBytes(fileName, position, buffer.array());
	}
}
