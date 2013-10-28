package ru.anisimov.storage.localStorage;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import ru.anisimov.storage.Storage;

import java.io.File;
import java.util.*;

import static org.junit.Assert.*;
import static org.junit.Assert.assertArrayEquals;

/**
 * @author Ivan Anisimov (ivananisimov2010@gmail.com)
 */
public class DirectoryStorageTest {
	private static final String RESOURCE_FILE_NAME = "/testStorage/testFile";
	private static final String TEST_FILE_NAME = FileBasedIndexTest.class.getResource(RESOURCE_FILE_NAME).getFile();
	private static final Random rnd = new Random(System.currentTimeMillis());
	private static String TEST_DIR_NAME = new File(TEST_FILE_NAME).getParent();

	@BeforeClass
	@AfterClass
	public static void tearDown() throws Exception {
		File dir = new File(TEST_DIR_NAME);
		File[] files = dir.listFiles();
		for (File file: files) {
			file.delete();
		}
		new File(TEST_FILE_NAME).createNewFile();
	}

	@Test
	public void testGenerateKey() throws Exception {
		Storage storage = DirectoryStorage.newStorage(TEST_DIR_NAME);
		Set<Long> used = new HashSet<>();
		for (int i = 0; i < 1000; i++) {
			long key = storage.generateKey();
			if (used.contains(key)) {
				fail("Not unique key: " + key);
			}
			used.add(key);
		}
	}

	@Test
	public void testWriteSingle() throws Exception {
		Storage storage = DirectoryStorage.newStorage(TEST_DIR_NAME);
		for (int i = 0; i < 1000; i++) {
			long ID = storage.generateKey();
			byte[] object = new byte[rnd.nextInt(100) + 1];
			rnd.nextBytes(object);
			storage.write(ID, object);
			assertArrayEquals(object, storage.get(ID));
		}
	}

	@Test
	public void testWriteMultiple() throws Exception {
		Storage storage = DirectoryStorage.newStorage(TEST_DIR_NAME);
		int testCount = 1000;
		long[] IDs = new long[testCount];
		byte[][] bytes = new byte[testCount][];
		for (int i = 0; i < testCount; i++) {
			IDs[i] = storage.generateKey();
			bytes[i] = new byte[rnd.nextInt(100) + 1];
			rnd.nextBytes(bytes[i]);
		}
		storage.write(IDs, bytes);
		assertArrayEquals(bytes, storage.get(IDs));
	}

	@Test
	public void testGetSingle() throws Exception {
		Storage storage = DirectoryStorage.newStorage(TEST_DIR_NAME);
		Map<Long, byte[]> objects = new HashMap<>();
		for (int i = 0; i < 1000; i++) {
			long ID = storage.generateKey();
			byte[] object = new byte[rnd.nextInt(100) + 1];
			rnd.nextBytes(object);
			storage.write(ID, object);
			objects.put(ID, object);
		}

		for (Long key: objects.keySet()) {
			assertArrayEquals(objects.get(key), storage.get(key));
		}
	}

	@Test
	public void testRemoveSingle() throws Exception {
		Storage storage = DirectoryStorage.newStorage(TEST_DIR_NAME);
		for (int i = 0; i < 1000; i++) {
			long ID = storage.generateKey();
			byte[] object = new byte[rnd.nextInt(100) + 1];
			rnd.nextBytes(object);
			storage.write(ID, object);
			assertArrayEquals(object, storage.get(ID));
			storage.remove(ID);
			assertNull(storage.get(ID));
		}
	}

	@Test
	public void testRemoveMultiple() throws Exception {
		Storage storage = DirectoryStorage.newStorage(TEST_DIR_NAME);
		int testCount = 1000;
		long[] IDs = new long[testCount];
		byte[][] bytes = new byte[testCount][];
		for (int i = 0; i < testCount; i++) {
			IDs[i] = storage.generateKey();
			bytes[i] = new byte[rnd.nextInt(100) + 1];
			rnd.nextBytes(bytes[i]);
		}
		storage.remove(IDs);
		byte[][] result = storage.get(IDs);
		for (int i = 0; i < testCount; i++) {
			assertNull(result[i]);
		}
	}

	@Test
	public void testKeyRemainsUniqueAfterRemove() throws Exception {
		Storage storage = DirectoryStorage.newStorage(TEST_DIR_NAME);

		int testCount = 1000;

		Set<Long> used = new HashSet<>();
		for (int i = 0; i < testCount; i++) {
			long ID = storage.generateKey();
			if (used.contains(ID)) {
				fail("Not unique key: " + ID);
			}
			used.add(ID);
			byte[] object = new byte[rnd.nextInt(100) + 1];
			rnd.nextBytes(object);
			storage.write(ID, object);
			storage.remove(ID);
		}
	}

	@Test
	public void testWorksOnOldDirectory() throws Exception {
		Storage storage = DirectoryStorage.newStorage(TEST_DIR_NAME);

		int testCount = 100;

		long[] IDs = new long[testCount];
		byte[][] objects = new byte[testCount][];
		for (int i = 0; i < testCount; i++) {
			IDs[i] = storage.generateKey();
			objects[i] = new byte[rnd.nextInt(100) + 1];
			rnd.nextBytes(objects[i]);
			storage.write(IDs[i], objects[i]);
		}

		storage = DirectoryStorage.getStorage(TEST_DIR_NAME);

		for (int i = 0; i < testCount / 2; i++) {
			storage.remove(IDs[i]);
			assertNull(storage.get(IDs[i]));
		}

		storage = DirectoryStorage.getStorage(TEST_DIR_NAME);

		for (int i = 0; i < testCount / 2; i++) {
			assertNull(storage.get(IDs[i]));
		}
		for (int i = testCount / 2; i < testCount; i++) {
			assertArrayEquals(objects[i], storage.get(IDs[i]));
		}
	}

	@Test
	public void testWorksAfterRebuild() throws Exception {
		Storage storage = DirectoryStorage.newStorage(TEST_DIR_NAME);

		int testCount = 1000;

		long[] IDs = new long[testCount];
		byte[][] objects = new byte[testCount][];
		for (int i = 0; i < testCount; i++) {
			IDs[i] = storage.generateKey();
			objects[i] = new byte[rnd.nextInt(100) + 1];
			rnd.nextBytes(objects[i]);
			storage.write(IDs[i], objects[i]);
		}

		for (int i = 0; i < testCount / 2; i++) {
			storage.remove(IDs[i]);
		}

		storage = DirectoryStorage.getStorage(TEST_DIR_NAME);
		storage.rebuild();

		for (int i = 0; i < testCount / 2; i++) {
			assertNull(storage.get(IDs[i]));
		}
		for (int i = testCount / 2; i < testCount; i++) {
			assertArrayEquals(objects[i], storage.get(IDs[i]));
		}
	}
}
