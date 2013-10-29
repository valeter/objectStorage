package ru.anisimov.storage;

import org.junit.*;
import ru.anisimov.storage.io.FileReaderWriter;
import ru.anisimov.storage.localStorage.DirectoryStorage;
import ru.anisimov.storage.localStorage.FileBasedIndexTest;

import java.io.BufferedWriter;
import java.io.File;
import java.io.PrintWriter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import static org.junit.Assert.*;

/**
 * @author Ivan Anisimov (ivananisimov2010@gmail.com)
 */
public class PerformanceTest {
	private static final String RESOURCE_FILE_NAME = "/testStorage/testFile";
	private static final String TEST_FILE_NAME = FileBasedIndexTest.class.getResource(RESOURCE_FILE_NAME).getFile();
	private static final Random rnd = new Random(System.currentTimeMillis());
	private static String TEST_DIR_NAME = new File(TEST_FILE_NAME).getParent();

	private static final int WRITE_SINGLE = 0;
	private static final int READ_SINGLE = 1;
	private static final int REMOVE_SINGLE = 2;
	private static final int WRITE_MULTIPLE = 3;
	private static final int READ_MULTIPLE = 4;
	private static final int REMOVE_MULTIPLE = 5;

	private static final int MULTIPLE_OPERATIONS_FOR_ROW = 1000;

	private static final int maxObjectSize = 1024 * 1024;

	private static final List<Long> keys = new ArrayList<>();
	private static final Map<Long, byte[]> hashes = new HashMap<>();

	private static Operation[] operations = new Operation[6];
	static {
		operations[WRITE_SINGLE] = new Operation() {
			@Override
			public long perform(Storage storage) throws Exception {
				byte[] object = new byte[rnd.nextInt(maxObjectSize)];
				rnd.nextBytes(object);

				long start = System.currentTimeMillis();
				long ID = storage.write(object);
				long time = System.currentTimeMillis() - start;

				assertFalse("Non unique ID generated", hashes.keySet().contains(ID));
				keys.add(ID);
				hashes.put(ID, MD5(object));

				return time;
			}
		};
		operations[READ_SINGLE] = new Operation() {
			@Override
			public long perform(Storage storage) throws Exception {
				long ID = keys.get(rnd.nextInt(keys.size()));

				long start = System.currentTimeMillis();
				byte[] object = storage.get(ID);
				long time = System.currentTimeMillis() - start;

				assertArrayEquals("Wrong object received from storage", MD5(object), hashes.get(ID));
				return time;
			}
		};
		operations[REMOVE_SINGLE] = new Operation() {
			@Override
			public long  perform(Storage storage) throws Exception {
				int keyInd = rnd.nextInt(keys.size());
				long ID = keys.get(keyInd);

				long start = System.currentTimeMillis();
				storage.remove(ID);
				long time = System.currentTimeMillis() - start;

				keys.remove(keyInd);
				hashes.remove(ID);
				return time;
			}
		};
		operations[WRITE_MULTIPLE] = new Operation() {
			@Override
			public long perform(Storage storage) throws Exception {
				byte[][] objects = new byte[MULTIPLE_OPERATIONS_FOR_ROW][];
				for (int i = 0; i < MULTIPLE_OPERATIONS_FOR_ROW; i++) {
					objects[i] = new byte[rnd.nextInt(maxObjectSize)];
					rnd.nextBytes(objects[i]);
				}

				long start = System.currentTimeMillis();
				long[] IDs = storage.write(objects);
				long time = System.currentTimeMillis() - start;

				for (int i = 0; i < MULTIPLE_OPERATIONS_FOR_ROW; i++) {
					assertFalse("Non unique ID generated", hashes.keySet().contains(IDs[i]));
				}

				for (int i = 0; i < MULTIPLE_OPERATIONS_FOR_ROW; i++) {
					keys.add(IDs[i]);
					hashes.put(IDs[i], MD5(objects[i]));
				}
				return time;
			}
		};
		operations[READ_MULTIPLE] = new Operation() {
			@Override
			public long perform(Storage storage) throws Exception {
				long[] IDs = new long[MULTIPLE_OPERATIONS_FOR_ROW];
				for (int i = 0; i < MULTIPLE_OPERATIONS_FOR_ROW; i++) {
					IDs[i] = keys.get(rnd.nextInt(keys.size()));
				}

				long start = System.currentTimeMillis();
				byte[][] object = storage.get(IDs);
				long time = System.currentTimeMillis() - start;

				for (int i = 0; i < MULTIPLE_OPERATIONS_FOR_ROW; i++) {
					assertArrayEquals("Wrong object received from storage", MD5(object[i]), hashes.get(IDs[i]));
				}
				return time;
			}
		};
		operations[REMOVE_MULTIPLE] = new Operation() {
			@Override
			public long  perform(Storage storage) throws Exception {
				int maxIDCount = Math.min(MULTIPLE_OPERATIONS_FOR_ROW, keys.size());
				int IDCount = rnd.nextInt(maxIDCount) + 1;
				long[] IDs = new long[IDCount];
				for (int i = 0; i < IDCount; i++) {
					int nextInd = rnd.nextInt(keys.size());
					IDs[i] = keys.get(nextInd);
					keys.remove(nextInd);
					hashes.remove(IDs[i]);
				}

				long start = System.currentTimeMillis();
				storage.remove(IDs);
				long time = System.currentTimeMillis() - start;

				for (int i = 0; i < IDCount; i++) {
					assertNull("Wrong object received from storage", storage.get(IDs[i]));
				}
				return time;
			}
		};
	}

	private BufferedWriter out = new BufferedWriter(new PrintWriter(System.out));

	@BeforeClass
	@AfterClass
	public static void clearFolder() throws Exception {
		System.gc();
		File dir = new File(TEST_DIR_NAME);
		File[] files = dir.listFiles();
		for (File file: files) {
			file.delete();
		}
		new File(TEST_FILE_NAME).createNewFile();
	}

	@BeforeClass
	public static void warmUp() throws Exception {
		long warmUpOperations = 100;
		Storage storage = DirectoryStorage.newStorage(TEST_DIR_NAME);
		for (int i = 0; i < warmUpOperations; i++) {
			if (keys.size() > 0) {
				operations[rnd.nextInt(3)].perform(storage);
			} else {
				operations[0].perform(storage);
			}
		}

		warmUpOperations = 5;
		for (int i = 0; i < warmUpOperations; i++) {
			if (keys.size() > 0) {
				operations[rnd.nextInt(3) + 3].perform(storage);
			} else {
				operations[3].perform(storage);
			}
		}
	}

	@After
	public void flushOutput() throws Exception {
		out.flush();
	}

	@Before
	public void clearKeys() {
		keys.clear();
		hashes.clear();
	}

	@Test
	public void testWritingSingeSpeed() throws Exception {
		out.write("Test writing single speed"); out.newLine();;
		out.newLine();

		int operationsCount = 1000;

		Storage storage = DirectoryStorage.newStorage(TEST_DIR_NAME);
		long time = 0;
		for (int i = 0; i < operationsCount; i++) {
			time += operations[WRITE_SINGLE].perform(storage);
		}
		out.write(operationsCount + " operations finished in: " + time + " ms"); out.newLine();;
		out.write("Average operation time: " + (time / operationsCount) + " ms"); out.newLine();;
		out.write("============================"); out.newLine();;
	}

	@Test
	public void testWritingMultipleSpeed() throws Exception {
		out.write("Test writing multiple speed"); out.newLine();;
		out.newLine();

		int operationsCount = 10;

		Storage storage = DirectoryStorage.newStorage(TEST_DIR_NAME);
		long time = 0;
		for (int i = 0; i < operationsCount; i++) {
			time += operations[WRITE_MULTIPLE].perform(storage);
		}
		out.write((operationsCount * MULTIPLE_OPERATIONS_FOR_ROW) + " operations finished in: " + time + " ms"); out.newLine();;
		out.write("Average operation time: " + (time / (operationsCount * MULTIPLE_OPERATIONS_FOR_ROW)) + " ms"); out.newLine();;
		out.write("============================"); out.newLine();;
	}

	@Test
	public void testReadingSingeSpeed() throws Exception {
		out.write("Test reading single speed"); out.newLine();;
		out.newLine();

		int operationsCount = 1000;

		Storage storage = DirectoryStorage.newStorage(TEST_DIR_NAME);
		for (int i = 0; i < 100; i++) {
			operations[WRITE_SINGLE].perform(storage);
		}
		long time = 0;
		for (int i = 0; i < operationsCount; i++) {
			time += operations[READ_SINGLE].perform(storage);
		}
		out.write(operationsCount + " operations finished in: " + time + " ms"); out.newLine();;
		out.write("Average operation time: " + (time / operationsCount) + " ms"); out.newLine();;
		out.write("============================"); out.newLine();;
	}

	@Test
	public void testReadingMultipleSpeed() throws Exception {
		out.write("Test reading multiple speed"); out.newLine();;
		out.newLine();

		int operationsCount = 10;

		Storage storage = DirectoryStorage.newStorage(TEST_DIR_NAME);
		for (int i = 0; i < 10; i++) {
			operations[WRITE_MULTIPLE].perform(storage);
		}
		long time = 0;
		for (int i = 0; i < operationsCount; i++) {
			time += operations[READ_MULTIPLE].perform(storage);
		}
		out.write((operationsCount * MULTIPLE_OPERATIONS_FOR_ROW) + " operations finished in: " + time + " ms"); out.newLine();;
		out.write("Average operation time: " + (time / (operationsCount * MULTIPLE_OPERATIONS_FOR_ROW)) + " ms"); out.newLine();;
		out.write("============================"); out.newLine();;
	}

	@Test
	public void testRemoveSingeSpeed() throws Exception {
		out.write("Test removing single speed"); out.newLine();;
		out.newLine();

		int operationsCount = 1000;

		Storage storage = DirectoryStorage.newStorage(TEST_DIR_NAME);
		for (int i = 0; i < operationsCount; i++) {
			operations[WRITE_SINGLE].perform(storage);
		}
		long time = 0;
		for (int i = 0; i < operationsCount; i++) {
			time += operations[REMOVE_SINGLE].perform(storage);
		}
		out.write(operationsCount + " operations finished in: " + time + " ms"); out.newLine();;
		out.write("Average operation time: " + (time / operationsCount) + " ms"); out.newLine();;
		out.write("============================"); out.newLine();;
	}

	@Test
	public void testRemoveMultipleSpeed() throws Exception {
		out.write("Test removing multiple speed"); out.newLine();;
		out.newLine();

		int operationsCount = 10;

		Storage storage = DirectoryStorage.newStorage(TEST_DIR_NAME);
		for (int i = 0; i < operationsCount; i++) {
			operations[WRITE_MULTIPLE].perform(storage);
		}
		long time = 0;
		for (int i = 0; i < operationsCount; i++) {
			time += operations[REMOVE_MULTIPLE].perform(storage);
		}
		out.write((operationsCount * MULTIPLE_OPERATIONS_FOR_ROW) + " operations finished in: " + time + " ms"); out.newLine();;
		out.write("Average operation time: " + (time / (operationsCount * MULTIPLE_OPERATIONS_FOR_ROW)) + " ms"); out.newLine();;
		out.write("============================"); out.newLine();;
	}

	@Test
	public void testRandomSingeOperationSpeed() throws Exception {
		out.write("Test random single operation speed"); out.newLine();;
		out.newLine();

		int operationsCount = 1000;

		Storage storage = DirectoryStorage.newStorage(TEST_DIR_NAME);
		long time = 0;
		for (int i = 0; i < operationsCount; i++) {
			Operation operation;
			if (keys.size() > 0) {
				operation = operations[rnd.nextInt(3)];
			} else {
				operation = operations[0];
			}
			time += operation.perform(storage);
		}
		out.write(operationsCount + " operations finished in: " + time + " ms"); out.newLine();;
		out.write("Average operation time: " + (time / operationsCount) + " ms"); out.newLine();;
		out.write("============================"); out.newLine();;
	}

	@Test
	public void testRandomMultipleOperationSpeed() throws Exception {
		out.write("Test random multiple operation speed"); out.newLine();;
		out.newLine();

		int operationsCount = 10;

		Storage storage = DirectoryStorage.newStorage(TEST_DIR_NAME);
		long time = 0;
		for (int i = 0; i < operationsCount; i++) {
			Operation operation;
			if (keys.size() > 0) {
				operation = operations[rnd.nextInt(3) + 3];
			} else {
				operation = operations[3];
			}
			time += operation.perform(storage);
		}
		out.write((operationsCount * MULTIPLE_OPERATIONS_FOR_ROW) + " operations finished in: " + time + " ms"); out.newLine();;
		out.write("Average operation time: " + (time / (operationsCount * MULTIPLE_OPERATIONS_FOR_ROW)) + " ms"); out.newLine();;
		out.write("============================"); out.newLine();;
	}

	@Test
	public void testSeeBestWritingPerformance() throws Exception {
		out.write("If nothing were done you could reach this writing speed:"); out.newLine();;
		out.newLine();

		int operationsCount = 1000;

		new File(TEST_FILE_NAME + ".temp").createNewFile();
		long position = 0;
		long time = 0;

		try (FileReaderWriter out = FileReaderWriter.openForWriting(TEST_FILE_NAME + ".temp")) {
			for (int i = 0; i < operationsCount; i++) {
				byte[] object = new byte[rnd.nextInt(maxObjectSize)];
				rnd.nextBytes(object);

				long start = System.currentTimeMillis();
				out.writeBytes(position);
				time += System.currentTimeMillis() - start;
				position += object.length;
			}
		}

		out.write(operationsCount + " operations finished in: " + time + " ms"); out.newLine();;
		out.write("Average operation time: " + (time / operationsCount) + " ms"); out.newLine();;
		out.write("============================"); out.newLine();;
	}

	private static byte[] MD5(byte[] bytes) throws NoSuchAlgorithmException {
		return MessageDigest.getInstance("MD5").digest(bytes);
	}

	private interface Operation {
		long perform(Storage storage) throws Exception;
	}
}
