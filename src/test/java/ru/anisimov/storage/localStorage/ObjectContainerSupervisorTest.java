package ru.anisimov.storage.localStorage;

import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * @author Ivan Anisimov (ivananisimov2010@gmail.com)
 */
public class ObjectContainerSupervisorTest {
	private static final String RESOURCE_FILE_NAME = "/testStorage/testFile";
	private static final String TEST_FILE_NAME = FileBasedIndexTest.class.getResource(RESOURCE_FILE_NAME).getFile();
	private static final Random rnd = new Random(System.currentTimeMillis());
	private static String TEST_DIR_NAME = new File(TEST_FILE_NAME).getParent();

	@After
	public void tearDown() throws Exception {
		System.out.println(TEST_DIR_NAME);
		File dir = new File(TEST_DIR_NAME);
		File[] files = dir.listFiles();
		for (File file: files) {
			file.delete();
		}
		new File(TEST_FILE_NAME).createNewFile();
	}

	@Test
	public void testRemove() throws Exception {
		ObjectContainerSupervisor supervisor = new ObjectContainerSupervisor(TEST_DIR_NAME, 1000);
		int testCount = 1000;
		ObjectAddress[] addresses = new ObjectAddress[testCount];
		byte[][] objects = new byte[testCount][];
		for (int i = 0; i < testCount; i++) {
			int size = rnd.nextInt((int)supervisor.getMaxObjectSize(1)) + 1;
			objects[i] = new byte[size];
			addresses[i] = supervisor.put(i, objects[i]);
		}

		for (int i = 0; i < testCount; i++) {
			supervisor.remove(addresses[i]);
			assertArrayEquals(null, supervisor.get(addresses[i]));
		}
	}

	@Test
	public void testPutAndGet() throws Exception {
		ObjectContainerSupervisor supervisor = new ObjectContainerSupervisor(TEST_DIR_NAME, 1000);
		int testCount = 1000;
		ObjectAddress[] addresses = new ObjectAddress[testCount];
		byte[][] objects = new byte[testCount][];
		for (int i = 0; i < testCount; i++) {
			int size = rnd.nextInt((int)supervisor.getMaxObjectSize(1)) + 1;
			objects[i] = new byte[size];
			addresses[i] = supervisor.put(i, objects[i]);
		}

		for (int i = 0; i < testCount; i++) {
			assertArrayEquals(objects[i], supervisor.get(addresses[i]));
		}
	}

	@Test
	public void testGetContainerCount() throws Exception {
		ObjectContainerSupervisor supervisor = new ObjectContainerSupervisor(TEST_DIR_NAME, 1000);
		int objectsCount = 10;
		ObjectAddress[] addresses = new ObjectAddress[objectsCount];
		for (int i = 0; i < objectsCount; i++) {
			addresses[i] = supervisor.put(i, new byte[(int) supervisor.getMaxObjectSize(1)]);
		}
		assertEquals(objectsCount, supervisor.getContainersCount());

		for (int i = 0; i < objectsCount; i++) {
			supervisor.remove(addresses[i]);
			supervisor.put(i, new byte[(int) supervisor.getMaxObjectSize(3)]);
		}
		assertEquals(objectsCount + (objectsCount + 2) / 3, supervisor.getContainersCount());
	}
}
