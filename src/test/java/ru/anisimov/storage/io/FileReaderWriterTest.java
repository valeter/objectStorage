package ru.anisimov.storage.io;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import ru.anisimov.storage.commons.TypeSizes;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
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
		try (BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(TEST_FILE_NAME))) {
			out.write(CONTENT);
		}
	}

	@After
	public void tearDown() throws Exception {
		new File(TEST_FILE_NAME).createNewFile();
	}

	@Test
	public void testReadSingleByte() throws Exception {
		for (int i = 0; i < CONTENT.length; i++) {
			byte expected = CONTENT[i];
			assertEquals(expected, FileReaderWriter.readByte(TEST_FILE_NAME, i));
		}
	}

	@Test
	public void testReadBytes() throws Exception {
		for (int i = 0; i < CONTENT.length; i++) {
			int bytesCount = rnd.nextInt(CONTENT.length - i) + 1;
			byte[] expected = Arrays.copyOfRange(CONTENT, i, i + bytesCount);
			assertArrayEquals(expected, FileReaderWriter.readByte(TEST_FILE_NAME, i, bytesCount));
		}
	}

	@Test
	public void testReadLong() throws Exception {
		for (int i = 0; i < CONTENT.length - TypeSizes.BYTES_IN_LONG; i++) {
			byte[] temp = Arrays.copyOfRange(CONTENT, i, i + TypeSizes.BYTES_IN_LONG);
			long expected = ByteBuffer.wrap(temp).getLong();
			assertEquals(expected, FileReaderWriter.readLong(TEST_FILE_NAME, i));
		}
	}

	@Test
	public void testWriteBytes() throws Exception {
		for (int i = 0; i < CONTENT.length; i++) {
			int bytesCount = rnd.nextInt(1000) + 1;
			byte[] expected = new byte[bytesCount];
			rnd.nextBytes(expected);
			FileReaderWriter.writeBytes(TEST_FILE_NAME, i, expected);
			byte[] actual = FileReaderWriter.readByte(TEST_FILE_NAME, i, bytesCount);
			assertArrayEquals(expected, actual);
		}
	}

	@Test
	public void testWriteZeroBytesNoException() throws Exception {
		FileReaderWriter.writeBytes(TEST_FILE_NAME, 0);
	}

	@Test
	public void testWriteToTheEnd() throws Exception {
		int bytesCount = rnd.nextInt(1000) + 1;
		byte[] expected = new byte[bytesCount];
		rnd.nextBytes(expected);
		FileReaderWriter.writeBytes(TEST_FILE_NAME, CONTENT.length, expected);
		byte[] actual = FileReaderWriter.readByte(TEST_FILE_NAME, CONTENT.length, bytesCount);
		assertArrayEquals(expected, actual);
	}

	@Test
	public void testWriteLongs() throws Exception {
		for (int i = 0; i < CONTENT.length; i++) {
			int longsCount = rnd.nextInt(1000) + 1;
			long[] longs = DataGenerator.generateLongs(longsCount);
			FileReaderWriter.writeLong(TEST_FILE_NAME, i, longs);
			for (int j = 0; j < longsCount; j++) {
				long expected = longs[j];
				long actual = FileReaderWriter.readLong(TEST_FILE_NAME, i + (j * TypeSizes.BYTES_IN_LONG));
				assertEquals(expected, actual);
			}
		}
	}

	@Test
	public void testWriteZeroLongsNoException() throws Exception {
		FileReaderWriter.writeLong(TEST_FILE_NAME, 0);
	}

	private static class DataGenerator {
		private DataGenerator() {
			throw new UnsupportedOperationException();
		}

		public static long[] generateLongs(int count) {
			long[] result = new long[count];

			for (int i = 0; i < count; i++) {
				result[i] = rnd.nextLong();
			}

			return result;
		}
	}
}
