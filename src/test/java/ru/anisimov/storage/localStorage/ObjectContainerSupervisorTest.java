package ru.anisimov.storage.localStorage;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import ru.anisimov.storage.commons.DataGenerator;

import java.io.File;
import java.util.Arrays;
import java.util.Random;

import static junit.framework.Assert.assertNull;
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

	@BeforeClass
	@AfterClass
	public static void tearDown() throws Exception {
		System.gc();
		File dir = new File(TEST_DIR_NAME);
		File[] files = dir.listFiles();
		for (File file: files) {
			if (!file.setWritable(true)) {
				throw new Exception();
			}
			file.delete();
		}
		new File(TEST_FILE_NAME).createNewFile();
	}

	@Test
	public void testRemoveSingle() throws Exception {
		ObjectContainerSupervisor supervisor = new ObjectContainerSupervisor(TEST_DIR_NAME, "", true, 1000);
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
			assertEquals(null, supervisor.get(addresses[i]));
		}
	}

	@Test
	public void testRemoveMultiple() throws Exception {
		ObjectContainerSupervisor supervisor = new ObjectContainerSupervisor(TEST_DIR_NAME, "", true, 1000);

		int testCount = 10000;

		byte[][] objects = new byte[testCount][];
		long[] IDs = new long[testCount];
		for (int i = 0; i < testCount; i++) {
			int size = rnd.nextInt((int)supervisor.getMaxObjectSize(1)) + 1;
			objects[i] = new byte[size];
			IDs[i] = i;
		}
		ObjectAddress[] addresses = supervisor.put(IDs, objects);

		supervisor.remove(addresses);
		RecordData[] result = supervisor.get(addresses);
		for (int i = 0; i < testCount; i++) {
			assertNull(result[i]);
		}
	}

	@Test
	public void testPutAndGetSingle() throws Exception {
		ObjectContainerSupervisor supervisor = new ObjectContainerSupervisor(TEST_DIR_NAME, "", true, 1000);
		int testCount = 10000;
		ObjectAddress[] addresses = new ObjectAddress[testCount];
		byte[][] objects = new byte[testCount][];
		for (int i = 0; i < testCount; i++) {
			int size = rnd.nextInt((int)supervisor.getMaxObjectSize(1)) + 1;
			objects[i] = new byte[size];
			rnd.nextBytes(objects[i]);
			addresses[i] = supervisor.put(i, objects[i]);
		}

		for (int i = 0; i < testCount; i++) {
			assertArrayEquals(objects[i], supervisor.get(addresses[i]).getObject());
		}
	}

	@Test
	public void testPutAndGetMultiple() throws Exception {
		ObjectContainerSupervisor supervisor = new ObjectContainerSupervisor(TEST_DIR_NAME, "", true, 1000);

		int testCount = 10000;

		byte[][] objects = new byte[testCount][];
		long[] IDs = new long[testCount];
		for (int i = 0; i < testCount; i++) {
			int size = rnd.nextInt((int)supervisor.getMaxObjectSize(1)) + 1;
			objects[i] = new byte[size];
			rnd.nextBytes(objects[i]);
			IDs[i] = i;
		}
		ObjectAddress[] addresses = supervisor.put(IDs, objects);
		RecordData[] result = supervisor.get(addresses);
		for (int i = 0; i < testCount; i++) {
			assertArrayEquals(objects[i], result[i].getObject());
		}
	}

	@Test
	public void testGetAfterRemoveMultiple() throws Exception {
		ObjectContainerSupervisor supervisor = new ObjectContainerSupervisor(TEST_DIR_NAME, "", true, 1000);

		int testCount = 1000;

		byte[][] objects = new byte[testCount][];
		long[] IDs = new long[testCount];
		for (int i = 0; i < testCount; i++) {
			int size = rnd.nextInt((int)supervisor.getMaxObjectSize(1)) + 1;
			objects[i] = new byte[size];
			IDs[i] = i;
		}
		ObjectAddress[] addresses = supervisor.put(IDs, objects);

		ObjectAddress[] removedAddresses = Arrays.copyOfRange(addresses, 0, testCount / 2);
		supervisor.remove(removedAddresses);
		RecordData[] result = supervisor.get(addresses);
		for (int i = 0; i < testCount / 2; i++) {
			assertNull(result[i]);
		}

		for (int i = testCount / 2; i < testCount; i++) {
			assertArrayEquals(objects[i], result[i].getObject());
		}
	}

	@Test
	public void testGetContainerCount() throws Exception {
		ObjectContainerSupervisor supervisor = new ObjectContainerSupervisor(TEST_DIR_NAME, "", true, 1000);
		int objectsCount = 1000;
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

	@Test
	public void testGetContainerCountDoesNotDependsOnObjectOrder() throws Exception {
		ObjectContainerSupervisor supervisor = new ObjectContainerSupervisor(TEST_DIR_NAME, "", true, 1000);
		int objectsCount = 1000;
		byte[][] objects = new byte[objectsCount * 2][];
		for (int i = 0; i < objectsCount; i++) {
			objects[i] = new byte[(int) supervisor.getMaxObjectSize(1)];
		}
		for (int i = objectsCount; i < objectsCount * 2; i++) {
			objects[i] = new byte[(int) supervisor.getMaxObjectSize(3)];
		}

		int[] order = DataGenerator.generateDifferentInts(objectsCount * 2);
		for (int i = 0; i < objectsCount * 2; i++) {
			int ind = order[i];
			supervisor.put(i, objects[ind]);
		}
		assertEquals(objectsCount + (objectsCount + 2) / 3, supervisor.getContainersCount());
	}

	@Test
	public void testWorksOnOldDirectory() throws Exception {
		ObjectContainerSupervisor supervisor = new ObjectContainerSupervisor(TEST_DIR_NAME, "testCont", true, 1000);

		int testCount = 1000;
		ObjectAddress[] addresses = new ObjectAddress[testCount];
		for (int i = 0; i < testCount; i++) {
			addresses[i] = supervisor.put(i, new byte[(int) supervisor.getMaxObjectSize(1)]);
		}
		assertEquals(testCount, supervisor.getContainersCount());

		supervisor = new ObjectContainerSupervisor(TEST_DIR_NAME, "testCont", false, 1000);

		for (int i = 0; i < testCount; i++) {
			supervisor.remove(addresses[i]);
			supervisor.put(i, new byte[(int) supervisor.getMaxObjectSize(3)]);
		}
		assertEquals(testCount + (testCount + 2) / 3, supervisor.getContainersCount());

		supervisor = new ObjectContainerSupervisor(TEST_DIR_NAME, "testCont", true, 1000);

		addresses = new ObjectAddress[testCount];
		byte[][] objects = new byte[testCount][];
		for (int i = 0; i < testCount; i++) {
			int size = rnd.nextInt((int)supervisor.getMaxObjectSize(1)) + 1;
			objects[i] = new byte[size];
			addresses[i] = supervisor.put(i, objects[i]);
		}

		supervisor = new ObjectContainerSupervisor(TEST_DIR_NAME, "testCont", false, 1000);

		for (int i = 0; i < testCount; i++) {
			assertArrayEquals(objects[i], supervisor.get(addresses[i]).getObject());
		}

		supervisor = new ObjectContainerSupervisor(TEST_DIR_NAME, "testCont", false, 1000);

		for (int i = 0; i < testCount; i++) {
			supervisor.remove(addresses[i]);
		}

		supervisor = new ObjectContainerSupervisor(TEST_DIR_NAME, "testCont", false, 1000);

		for (int i = 0; i < testCount; i++) {
			assertNull(supervisor.get(addresses[i]));
		}
	}
}
