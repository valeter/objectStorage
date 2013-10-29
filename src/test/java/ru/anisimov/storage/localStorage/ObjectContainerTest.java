package ru.anisimov.storage.localStorage;

import org.junit.AfterClass;
import org.junit.Test;
import ru.anisimov.storage.io.FileReaderWriter;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * @author Ivan Anisimov (ivananisimov2010@gmail.com)
 */
public class ObjectContainerTest {
	private static final String RESOURCE_FILE_NAME = "objectContainerTest";
	private static final String TEST_FILE_NAME = FileBasedIndexTest.class.getResource(RESOURCE_FILE_NAME).getFile();
	private static final Random rnd = new Random(System.currentTimeMillis());

	@AfterClass
	public static void tearDown() throws Exception {
		new File(TEST_FILE_NAME).createNewFile();
	}

	@Test
	public void testRemoveSingle() throws Exception {
		try (FileReaderWriter rw = FileReaderWriter.openForReadingWriting(TEST_FILE_NAME)) {
			ObjectContainer container = new ObjectContainer(rw, TEST_FILE_NAME, 0, true);

			int testCount = 1000;
			List<ObjectAddress> addresses = new ArrayList<>(testCount);
			for (int i = 0; i < testCount; i++) {
				byte[] bytes = new byte[rnd.nextInt(100) + 1];
				addresses.add(container.writeBytes(rw, i, bytes));
			}

			for (ObjectAddress address : addresses) {
				container.removeBytes(rw, address.getFilePosition());
			}
		}
	}

	@Test
	public void testRemoveMultiple() throws Exception {
		try (FileReaderWriter rw = FileReaderWriter.openForReadingWriting(TEST_FILE_NAME)) {
			ObjectContainer container = new ObjectContainer(rw, TEST_FILE_NAME, 0, true);

			int testCount = 10000;
			byte[][] bytes = new byte[testCount][];
			long[] IDs = new long[testCount];
			for (int i = 0; i < testCount; i++) {
				bytes[i] = new byte[rnd.nextInt(100) + 1];
				IDs[i] = i;
			}
			ObjectAddress[] addresses = container.writeBytes(rw, IDs, bytes);
			long[] positions = new long[addresses.length];
			for (int i = 0; i < addresses.length; i++) {
				positions[i] = addresses[i].getFilePosition();
			}
			container.removeBytes(rw, positions);

			RecordData[] result = container.getData(rw, positions);
			for (int i = 0; i < result.length; i++) {
				assertNull(result[i]);
			}
		}
	}

	@Test
	public void testWriteAndReadSingle() throws Exception {
		try (FileReaderWriter rw = FileReaderWriter.openForReadingWriting(TEST_FILE_NAME)) {
			ObjectContainer container = new ObjectContainer(rw, TEST_FILE_NAME, 0, true);

			int testCount = 1000;
			List<ObjectAddress> addresses = new ArrayList<>(testCount);
			List<byte[]> expectedBytes = new ArrayList<>();

			for (int i = 0; i < testCount; i++) {
				byte[] bytes = new byte[rnd.nextInt(100) + 1];
				Arrays.fill(bytes, (byte) 1);
				expectedBytes.add(bytes);
				addresses.add(container.writeBytes(rw, i, bytes));
			}

			for (int i = 0; i < testCount; i++) {
				byte[] actualBytes = container.getData(rw, addresses.get(i).getFilePosition()).getObject();
				assertArrayEquals(expectedBytes.get(i), actualBytes);
			}
		}
	}

	@Test
	public void testWriteAndReadMultiple() throws Exception {
		try (FileReaderWriter rw = FileReaderWriter.openForReadingWriting(TEST_FILE_NAME)) {
			ObjectContainer container = new ObjectContainer(rw, TEST_FILE_NAME, 0, true);

			int testCount = 10000;
			byte[][] bytes = new byte[testCount][];
			long[] IDs = new long[testCount];
			for (int i = 0; i < testCount; i++) {
				bytes[i] = new byte[rnd.nextInt(100) + 1];
				IDs[i] = i;
			}
			for (int i = 0; i < testCount; i++) {
				bytes[i] = new byte[rnd.nextInt(100) + 1];
				Arrays.fill(bytes[i], (byte) 1);
				IDs[i] = i;
			}
			ObjectAddress[] addresses = container.writeBytes(rw, IDs, bytes);
			long[] positions = new long[addresses.length];
			for (int i = 0; i < addresses.length; i++) {
				positions[i] = addresses[i].getFilePosition();
			}

			RecordData[] result = container.getData(rw, positions);
			for (int i = 0; i < testCount; i++) {
				byte[] actualBytes = result[i].getObject();
				assertArrayEquals(bytes[i], actualBytes);
			}
		}
	}

	@Test
	public void testGetAfterRemove() throws Exception {
		try (FileReaderWriter rw = FileReaderWriter.openForReadingWriting(TEST_FILE_NAME)) {
			ObjectContainer container = new ObjectContainer(rw, TEST_FILE_NAME, 0, true);

			int testCount = 1000;
			List<ObjectAddress> addresses = new ArrayList<>(testCount);
			List<byte[]> expectedBytes = new ArrayList<>();
			for (int i = 0; i < testCount; i++) {
				byte[] bytes = new byte[rnd.nextInt(100) + 1];
				expectedBytes.add(bytes);
				addresses.add(container.writeBytes(rw, i, bytes));
			}

			for (int i = 0; i < testCount / 2; i++) {
				container.removeBytes(rw, addresses.get(i).getFilePosition());
				assertEquals(null, container.getData(rw, addresses.get(i).getFilePosition()));
			}

			ObjectAddress a = container.writeBytes(rw, 0, new byte[0]);
			assertArrayEquals(new byte[0], container.getData(rw, a.getFilePosition()).getObject());

			for (int i = testCount / 2; i < testCount; i++) {
				byte[] actualBytes = container.getData(rw, addresses.get(i).getFilePosition()).getObject();
				assertArrayEquals(expectedBytes.get(i), actualBytes);
			}
		}
	}

	@Test
	public void testRecordsCountAfterSingle() throws Exception {
		try (FileReaderWriter rw = FileReaderWriter.openForReadingWriting(TEST_FILE_NAME)) {
			ObjectContainer container = new ObjectContainer(rw, TEST_FILE_NAME, 0, true);

			int testCount = 1000;
			List<ObjectAddress> addresses = new ArrayList<>(testCount);
			List<byte[]> expectedBytes = new ArrayList<>();
			for (int i = 0; i < testCount; i++) {
				byte[] bytes = new byte[rnd.nextInt(100) + 1];
				expectedBytes.add(bytes);
				addresses.add(container.writeBytes(rw, i, bytes));
			}

			for (int i = 0; i < testCount / 2; i++) {
				container.removeBytes(rw, addresses.get(i).getFilePosition());
				assertEquals(testCount - i - 1, container.getRecordsCount());
			}
		}
	}

	@Test
	public void testRecordsCountAfterMultiple() throws Exception {
		try (FileReaderWriter rw = FileReaderWriter.openForReadingWriting(TEST_FILE_NAME)) {
			ObjectContainer container = new ObjectContainer(rw, TEST_FILE_NAME, 0, true);

			int testCount = 10000;
			byte[][] bytes = new byte[testCount][];
			long[] IDs = new long[testCount];
			for (int i = 0; i < testCount; i++) {
				bytes[i] = new byte[rnd.nextInt(100) + 1];
				IDs[i] = i;
			}
			ObjectAddress[] addresses = container.writeBytes(rw, IDs, bytes);

			assertEquals(testCount, container.getRecordsCount());

			long[] positions = new long[addresses.length];
			for (int i = 0; i < addresses.length; i++) {
				positions[i] = addresses[i].getFilePosition();
			}
			container.removeBytes(rw, positions);

			assertEquals(0, container.getRecordsCount());
		}
	}

	@Test
	public void testGetRecordsAddresses() throws Exception {
		try (FileReaderWriter rw = FileReaderWriter.openForReadingWriting(TEST_FILE_NAME)) {
			ObjectContainer container = new ObjectContainer(rw, TEST_FILE_NAME, 0, true);

			int testCount = 100;

			List<ObjectAddress> addresses = new ArrayList<>(testCount);
			for (int i = 0; i < testCount; i++) {
				byte[] bytes = new byte[rnd.nextInt(100) + 1];
				addresses.add(container.writeBytes(rw, i, bytes));
			}

			assertEquals(addresses, container.getRecordsAddresses(rw));

			for (int i = 0; i < testCount / 2; i++) {
				container.removeBytes(rw, addresses.get(0).getFilePosition());
				addresses.remove(0);
			}

			assertEquals(addresses, container.getRecordsAddresses(rw));

			for (int i = 0; i < testCount; i++) {
				byte[] bytes = new byte[rnd.nextInt(100) + 1];
				addresses.add(container.writeBytes(rw, i, bytes));
			}

			assertEquals(addresses, container.getRecordsAddresses(rw));
		}
	}

	@Test
	public void testWorksOnOldFile() throws Exception {
		try (FileReaderWriter rw = FileReaderWriter.openForReadingWriting(TEST_FILE_NAME)) {
			ObjectContainer container = new ObjectContainer(rw, TEST_FILE_NAME, 0, true);

			int testCount = 1000;
			List<ObjectAddress> addresses = new ArrayList<>(testCount);
			List<byte[]> expectedBytes = new ArrayList<>();
			for (int i = 0; i < testCount; i++) {
				byte[] bytes = new byte[rnd.nextInt(100) + 1];
				expectedBytes.add(bytes);
				addresses.add(container.writeBytes(rw, i, bytes));
			}

			container = new ObjectContainer(rw, TEST_FILE_NAME, 0, false);

			for (int i = 0; i < testCount / 2; i++) {
				container.removeBytes(rw, addresses.get(i).getFilePosition());
				assertEquals(testCount - i - 1, container.getRecordsCount());
			}

			container = new ObjectContainer(rw, TEST_FILE_NAME, 0, true);

			addresses = new ArrayList<>(testCount);
			expectedBytes = new ArrayList<>();
			for (int i = 0; i < testCount; i++) {
				byte[] bytes = new byte[rnd.nextInt(100) + 1];
				expectedBytes.add(bytes);
				addresses.add(container.writeBytes(rw, i, bytes));
			}

			container = new ObjectContainer(rw, TEST_FILE_NAME, 0, false);

			for (int i = 0; i < testCount / 2; i++) {
				container.removeBytes(rw, addresses.get(i).getFilePosition());
				assertEquals(null, container.getData(rw, addresses.get(i).getFilePosition()));
			}

			container = new ObjectContainer(rw, TEST_FILE_NAME, 0, false);

			ObjectAddress a = container.writeBytes(rw, 0, new byte[0]);
			assertArrayEquals(new byte[0], container.getData(rw, a.getFilePosition()).getObject());

			for (int i = testCount / 2; i < testCount; i++) {
				byte[] actualBytes = container.getData(rw, addresses.get(i).getFilePosition()).getObject();
				assertArrayEquals(expectedBytes.get(i), actualBytes);
			}
		}
	}
}
