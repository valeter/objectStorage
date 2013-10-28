package ru.anisimov.storage.localStorage;

import org.junit.AfterClass;
import org.junit.Test;
import ru.anisimov.storage.exceptions.IDGeneratorException;

import java.io.File;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author Ivan Anisimov (ivananisimov2010@gmail.com)
 */
public class FileBasedIDGeneratorTest {
	private static final String RESOURCE_FILE_NAME = "idGeneratorTest";
	private static final String TEST_FILE_NAME = FileBasedIDGeneratorTest.class.getResource(RESOURCE_FILE_NAME).getFile();
	private static final Random rnd = new Random(System.currentTimeMillis());

	@AfterClass
	public static void tearDown() throws Exception {
		new File(TEST_FILE_NAME).createNewFile();
	}

	@Test
	public void testOnNonExistingFile() throws Exception {
		FileBasedIDGenerator generator = new FileBasedIDGenerator(TEST_FILE_NAME + ".temp", true, 0, 1000);

		for (int i = 0; i <= 1000; i++) {
			generator.generateID();
		}

		new File(TEST_FILE_NAME + ".temp").delete();
	}

	@Test
	public void testGenerateID() throws Exception {
		FileBasedIDGenerator generator = new FileBasedIDGenerator(TEST_FILE_NAME, true, 0, 1000);
		Set<Long> uniqueIDs = new HashSet<>();

		for (int i = 0; i <= 1000; i++) {
			Long nextID = generator.generateID();
			if (uniqueIDs.contains(nextID)) {
				fail("Generated ID is not unique");
			}
			uniqueIDs.add(nextID);
		}
	}

	@Test
	public void testIDRemainsUniqueAfterFree() throws Exception {
		FileBasedIDGenerator generator = new FileBasedIDGenerator(TEST_FILE_NAME, true, 0, 1000);
		Set<Long> uniqueIDs = new HashSet<>();

		for (int i = 0; i <= 1000; i++) {
			generator.generateID();
		}

		for (int i = 0; i <= 1000; i++) {
			generator.addFreeID(i);
		}

		for (int i = 0; i <= 1000; i++) {
			Long nextID = generator.generateID();
			if (uniqueIDs.contains(nextID)) {
				fail("Generated ID is not unique");
			}
			uniqueIDs.add(nextID);
		}
	}

	@Test(expected = IDGeneratorException.class)
	public void testTooManyIDs() throws Exception {
		FileBasedIDGenerator generator = new FileBasedIDGenerator(TEST_FILE_NAME, true, 0, 1000);

		for (int i = 0; i <= 1001; i++) {
			generator.generateID();
		}
	}

	@Test(expected = IDGeneratorException.class)
	public void testTooManyIDsAfterRandomFreeCount() throws Exception {
		FileBasedIDGenerator generator = new FileBasedIDGenerator(TEST_FILE_NAME, true, 0, 1000);

		for (int i = 0; i <= 1000; i++) {
			generator.generateID();
		}

		int freeIdCount = rnd.nextInt(1000) + 1;
		for (int i = 0; i < freeIdCount; i++) {
			generator.addFreeID(i);
		}
		for (int i = 0; i <= freeIdCount; i++) {
			generator.generateID();
		}
	}

	@Test(expected = IDGeneratorException.class)
	public void testTooManyIDsAfterSingleFree() throws Exception {
		FileBasedIDGenerator generator = new FileBasedIDGenerator(TEST_FILE_NAME, true, 0, 1000);

		for (int i = 0; i <= 1000; i++) {
			generator.generateID();
		}

		generator.addFreeID(0);
		generator.generateID();
		generator.generateID();
	}

	@Test
	public void testFreeID() throws Exception {
		FileBasedIDGenerator generator = new FileBasedIDGenerator(TEST_FILE_NAME, true, 0, 1000);

		for (int i = 0; i <= 1000; i++) {
			generator.generateID();
		}

		for (long i = 0; i <= 1000; i++) {
			generator.addFreeID(i);
			assertEquals(i, generator.generateID());
		}
	}

	@Test
	public void testWorksOnOldFile() throws Exception {
		FileBasedIDGenerator generator = new FileBasedIDGenerator(TEST_FILE_NAME, true, 0, 1000);

		for (int i = 0; i <= 1000; i++) {
			generator.generateID();
		}

		FileBasedIDGenerator newGenerator = new FileBasedIDGenerator(TEST_FILE_NAME, false);
		for (long i = 0; i <= 1000; i++) {
			newGenerator.addFreeID(i);
			assertEquals(i, generator.generateID());
		}
	}
}
