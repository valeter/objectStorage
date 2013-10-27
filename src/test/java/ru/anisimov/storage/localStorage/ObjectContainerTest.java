package ru.anisimov.storage.localStorage;

import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * @author Ivan Anisimov (ivananisimov2010@gmail.com)
 */
public class ObjectContainerTest {
	private static final String RESOURCE_FILE_NAME = "objectContainerTest";
	private static final String TEST_FILE_NAME = FileBasedIndexTest.class.getResource(RESOURCE_FILE_NAME).getFile();
	private static final Random rnd = new Random(System.currentTimeMillis());

	@After
	public void tearDown() throws Exception {
		new File(TEST_FILE_NAME).createNewFile();
	}

	@Test
	public void testRemoveBytes() throws Exception {
		ObjectContainer container = new ObjectContainer(TEST_FILE_NAME, 0, true);

		int testCount = 1000;
		List<ObjectAddress> addresses = new ArrayList<>(testCount);
		for (int i = 0; i < testCount; i++) {
			byte[] bytes = new byte[rnd.nextInt(100) + 1];
			addresses.add(container.writeBytes(i, bytes));
		}

		for (ObjectAddress address : addresses) {
			container.removeBytes(address.getFilePosition());
		}
	}

	@Test
	public void testWriteAndReadBytes() throws Exception {
		ObjectContainer container = new ObjectContainer(TEST_FILE_NAME, 0, true);

		int testCount = 10;
		List<ObjectAddress> addresses = new ArrayList<>(testCount);
		List<byte[]> expectedBytes = new ArrayList<>();

		for (int i = 0; i < testCount; i++) {
			byte[] bytes = new byte[rnd.nextInt(100) + 1];
			Arrays.fill(bytes, (byte) 1);
			expectedBytes.add(bytes);
			addresses.add(container.writeBytes(i, bytes));
		}

		for (int i = 0; i < testCount; i++) {
			byte[] actualBytes = container.getBytes(addresses.get(i).getFilePosition());
			assertArrayEquals(expectedBytes.get(i), actualBytes);
		}
	}

	@Test
	public void testGetAfterRemove() throws Exception {
		ObjectContainer container = new ObjectContainer(TEST_FILE_NAME, 0, true);

		int testCount = 1000;
		List<ObjectAddress> addresses = new ArrayList<>(testCount);
		List<byte[]> expectedBytes = new ArrayList<>();
		for (int i = 0; i < testCount; i++) {
			byte[] bytes = new byte[rnd.nextInt(100) + 1];
			expectedBytes.add(bytes);
			addresses.add(container.writeBytes(i, bytes));
		}

		for (int i = 0; i < testCount / 2; i++) {
			container.removeBytes(addresses.get(i).getFilePosition());
			byte[] actualBytes = container.getBytes(addresses.get(i).getFilePosition());
			assertEquals(null, actualBytes);
		}

		ObjectAddress a = container.writeBytes(0, new byte[0]);
		assertArrayEquals(new byte[0], container.getBytes(a.getFilePosition()));

		for (int i = testCount / 2; i < testCount; i++) {
			byte[] actualBytes = container.getBytes(addresses.get(i).getFilePosition());
			assertArrayEquals(expectedBytes.get(i), actualBytes);
		}
	}

	@Test
	public void testRecordsCount() throws Exception {
		ObjectContainer container = new ObjectContainer(TEST_FILE_NAME, 0, true);

		int testCount = 1000;
		List<ObjectAddress> addresses = new ArrayList<>(testCount);
		List<byte[]> expectedBytes = new ArrayList<>();
		for (int i = 0; i < testCount; i++) {
			byte[] bytes = new byte[rnd.nextInt(100) + 1];
			expectedBytes.add(bytes);
			addresses.add(container.writeBytes(i, bytes));
		}

		for (int i = 0; i < testCount / 2; i++) {
			container.removeBytes(addresses.get(i).getFilePosition());
			byte[] actualBytes = container.getBytes(addresses.get(i).getFilePosition());
			assertEquals(testCount - i - 1, container.parseRecordsCount());
		}
	}
}
