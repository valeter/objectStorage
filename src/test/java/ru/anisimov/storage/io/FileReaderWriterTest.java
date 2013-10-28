package ru.anisimov.storage.io;

import org.junit.Before;
import org.junit.Test;
import ru.anisimov.storage.commons.DataGenerator;
import ru.anisimov.storage.commons.TypeSizes;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * @author Ivan Anisimov (ivananisimov2010@gmail.com)
 */
public class FileReaderWriterTest {
	private static final String RESOURCE_FILE_NAME = "fileReaderTest";
	private static final String TEST_FILE_NAME = FileReaderWriterTest.class.getResource(RESOURCE_FILE_NAME).getFile();
	private static final Random rnd = new Random(System.currentTimeMillis());

	private final byte[] CONTENT = {
										   4, 3, 2, 7, 1, 0, 10, 5,
										   -2, 1, 2, 7, 0, 3, 2, 1,
										   8, 4, 100, -11, 12, 45, 51
	};

	@Before
	public void setUp() throws Exception {
		try (FileReaderWriter out = FileReaderWriter.openForWriting(TEST_FILE_NAME)) {
			out.writeBytes(0, CONTENT);
		}
	}

	@Test
	public void testReadSingleByte() throws Exception {
		try (FileReaderWriter in = FileReaderWriter.openForReading(TEST_FILE_NAME)) {
			for (int i = 0; i < CONTENT.length; i++) {
				byte expected = CONTENT[i];
				assertEquals(expected, in.readByte(i));
			}
		}
	}

	@Test
	public void testReadBytes() throws Exception {
		try (FileReaderWriter in = FileReaderWriter.openForReading(TEST_FILE_NAME)) {
			for (int i = 0; i < CONTENT.length; i++) {
				int bytesCount = rnd.nextInt(CONTENT.length - i) + 1;
				byte[] expected = Arrays.copyOfRange(CONTENT, i, i + bytesCount);
				assertArrayEquals(expected, in.readByte(i, bytesCount));
			}
		}
	}

	@Test
	public void testReadLong() throws Exception {
		try (FileReaderWriter in = FileReaderWriter.openForReading(TEST_FILE_NAME)) {
			for (int i = 0; i < CONTENT.length - TypeSizes.BYTES_IN_LONG; i++) {
				byte[] temp = Arrays.copyOfRange(CONTENT, i, i + TypeSizes.BYTES_IN_LONG);
				long expected = ByteBuffer.wrap(temp).getLong();
				assertEquals(expected, in.readLong(i));
			}
		}
	}

	@Test
	public void testWriteBytes() throws Exception {
		try (FileReaderWriter rw = FileReaderWriter.openForReadingWriting(TEST_FILE_NAME)) {
			for (int i = 0; i < CONTENT.length; i++) {
				int bytesCount = rnd.nextInt(1000) + 1;
				byte[] expected = new byte[bytesCount];
				rnd.nextBytes(expected);
				rw.writeBytes(i, expected);
				byte[] actual = rw.readByte(i, bytesCount);
				assertArrayEquals(expected, actual);
			}
		}
	}

	@Test
	public void testWriteZeroBytesNoException() throws Exception {
		try (FileReaderWriter out =  FileReaderWriter.openForWriting(TEST_FILE_NAME)) {
			out.writeBytes(0);
		}
	}

	@Test
	public void testWriteToTheEnd() throws Exception {
		try (FileReaderWriter rw = FileReaderWriter.openForReadingWriting(TEST_FILE_NAME)) {
			int bytesCount = rnd.nextInt(1000) + 1;
			byte[] expected = new byte[bytesCount];
			rnd.nextBytes(expected);
			rw.writeBytes(CONTENT.length, expected);
			byte[] actual = rw.readByte(CONTENT.length, bytesCount);
			assertArrayEquals(expected, actual);
		}
	}

	@Test
	public void testWriteLongs() throws Exception {
		try (FileReaderWriter rw = FileReaderWriter.openForReadingWriting(TEST_FILE_NAME)) {
			for (int i = 0; i < CONTENT.length; i++) {
				int longsCount = rnd.nextInt(1000) + 1;
				long[] longs = DataGenerator.generateLongs(longsCount);
				for (int j = 0; j < longsCount; j++) {
					rw.writeLong(i + (j * TypeSizes.BYTES_IN_LONG), longs[j]);
				}
				for (int j = 0; j < longsCount; j++) {
					long expected = longs[j];
					long actual = rw.readLong(i + (j * TypeSizes.BYTES_IN_LONG));
					assertEquals(expected, actual);
				}
			}
		}
	}

	@Test
	public void testWriteInts() throws Exception {
		try (FileReaderWriter rw = FileReaderWriter.openForReadingWriting(TEST_FILE_NAME)) {
			for (int i = 0; i < CONTENT.length; i++) {
				int intsCount = rnd.nextInt(1000) + 1;
				int[] ints = DataGenerator.generateInts(intsCount);
				for (int j = 0; j < intsCount; j++) {
					rw.writeInt(i + (j * TypeSizes.BYTES_IN_INT), ints[j]);
				}
				for (int j = 0; j < intsCount; j++) {
					int expected = ints[j];
					int actual = rw.readInt(i + (j * TypeSizes.BYTES_IN_INT));
					assertEquals(expected, actual);
				}
			}
		}
	}
}
