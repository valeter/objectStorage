package ru.anisimov.storage.localStorage;

import org.junit.After;
import org.junit.Test;
import ru.anisimov.storage.commons.DataGenerator;

import java.io.File;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * @author Ivan Anisimov (ivananisimov2010@gmail.com)
 */
public class FileBasedIndexTest {
	private static final String RESOURCE_FILE_NAME = "indexTest";
	private static final String TEST_FILE_NAME = FileBasedIndexTest.class.getResource(RESOURCE_FILE_NAME).getFile();

	@After
	public void tearDown() throws Exception {
		new File(TEST_FILE_NAME).createNewFile();
	}

	@Test
	public void testOnNonExistingFile() throws Exception {
		FileBasedIndex index = new FileBasedIndex(TEST_FILE_NAME + ".temp", true, 1000);

		for (int i = 0; i <= 1000; i++) {
			index.getAddress(i);
		}

		new File(TEST_FILE_NAME + ".temp").delete();
	}

	@Test
	public void testGetAddress() throws Exception {
		FileBasedIndex index = new FileBasedIndex(TEST_FILE_NAME, true, 1000);
		assertEquals(ObjectAddress.EMPTY_ADDRESS, index.getAddress(-1));
	}

	@Test
	public void testPutAddress() throws Exception {
		FileBasedIndex index = new FileBasedIndex(TEST_FILE_NAME, true, 1000);

		int testCount = 1000;

		long[] IDs = DataGenerator.generateDifferentLongs(testCount);
		ObjectAddress[] addresses = new ObjectAddress[testCount];
		DataGenerator.generateObjects(addresses, new DataGenerator.ObjectGenerator<ObjectAddress>() {
			@Override
			public ObjectAddress generate(Random rnd) {
				return new ObjectAddress(rnd.nextLong(), rnd.nextLong(), rnd.nextLong());
			}
		});

		for (int i = 0; i < testCount; i++) {
			index.putAddress(IDs[i], addresses[i]);
		}

		for (int i = 0; i < testCount; i++) {
			assertEquals(addresses[i], index.getAddress(IDs[i]));
		}
	}

	@Test
	public void testSameIDPutRewritesAddress() throws Exception {
		FileBasedIndex index = new FileBasedIndex(TEST_FILE_NAME, true, 1000);

		int testCount = 1000;

		long[] IDs = new long[testCount];
		Arrays.fill(IDs, -1);
		ObjectAddress[] addresses = new ObjectAddress[testCount];
		DataGenerator.generateObjects(addresses, new DataGenerator.ObjectGenerator<ObjectAddress>() {
			@Override
			public ObjectAddress generate(Random rnd) {
				return new ObjectAddress(rnd.nextLong(), rnd.nextLong(), rnd.nextLong());
			}
		});

		for (int i = 0; i < testCount; i++) {
			index.putAddress(IDs[i], addresses[i]);
		}

		assertEquals(addresses[addresses.length - 1], index.getAddress(IDs[0]));
	}

	@Test
	public void testRemoveAddress() throws Exception {
		FileBasedIndex index = new FileBasedIndex(TEST_FILE_NAME, true, 1000);

		int testCount = 1000;

		long[] IDs = DataGenerator.generateDifferentLongs(testCount);
		ObjectAddress[] addresses = new ObjectAddress[testCount];
		DataGenerator.generateObjects(addresses, new DataGenerator.ObjectGenerator<ObjectAddress>() {
			@Override
			public ObjectAddress generate(Random rnd) {
				return new ObjectAddress(rnd.nextLong(), rnd.nextLong(), rnd.nextLong());
			}
		});

		for (int i = 0; i < testCount; i++) {
			index.putAddress(IDs[i], addresses[i]);
		}

		for (int i = 0; i < testCount; i++) {
			index.removeAddress(IDs[i]);
		}

		for (int i = 0; i < testCount; i++) {
			assertEquals(ObjectAddress.EMPTY_ADDRESS, index.getAddress(IDs[i]));
		}
	}

	@Test
	public void testPutAfterRemove() throws Exception {
		FileBasedIndex index = new FileBasedIndex(TEST_FILE_NAME, true, 1000);

		int testCount = 1000;

		long[] IDs = DataGenerator.generateDifferentLongs(testCount);
		ObjectAddress[] addresses = new ObjectAddress[testCount];
		DataGenerator.generateObjects(addresses, new DataGenerator.ObjectGenerator<ObjectAddress>() {
			@Override
			public ObjectAddress generate(Random rnd) {
				return new ObjectAddress(rnd.nextLong(), rnd.nextLong(), rnd.nextLong());
			}
		});

		for (int i = 0; i < testCount; i++) {
			index.putAddress(IDs[i], addresses[i]);
		}

		for (int i = 0; i < testCount / 2; i++) {
			index.removeAddress(IDs[i]);
		}

		for (int i = testCount / 2; i < testCount; i++) {
			assertEquals(addresses[i], index.getAddress(IDs[i]));
		}

		for (int i = 0; i < testCount / 2; i++) {
			index.putAddress(IDs[i], addresses[i]);
		}

		for (int i = 0; i < testCount; i++) {
			assertEquals(addresses[i], index.getAddress(IDs[i]));
		}
	}
}
