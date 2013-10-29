package ru.anisimov.storage;

import org.junit.Ignore;
import org.junit.Test;
import ru.anisimov.storage.commons.TypeSizes;
import ru.anisimov.storage.localStorage.DirectoryStorage;
import ru.anisimov.storage.localStorage.FileBasedIndexTest;

import java.io.BufferedWriter;
import java.io.File;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import static org.junit.Assert.*;

/**
 * @author Ivan Anisimov (ivananisimov2010@gmail.com)
 */
public class LoadingTest {
	private static final String RESOURCE_FILE_NAME = "/testStorage/testFile";
	private static final String TEST_FILE_NAME = FileBasedIndexTest.class.getResource(RESOURCE_FILE_NAME).getFile();
	private static final Random rnd = new Random(System.currentTimeMillis());
	private static String TEST_DIR_NAME = new File(TEST_FILE_NAME).getParent();

	private BufferedWriter out = new BufferedWriter(new PrintWriter(System.out));

	@Test
	@Ignore
	public void bigTest() throws Exception {
		int maxObjectSize = 1024;
		int totalObjectCount = 100_000;
		int objectPackSize = totalObjectCount / 10;

		writeStartMessage();

		Storage storage = DirectoryStorage.newStorage(TEST_DIR_NAME);
		long[] ID = new long[totalObjectCount];
		// It is faster to work with storage with packs of objects
		for (int i = 0; i < totalObjectCount; i += objectPackSize) {
			long[] tempID = storage.generateKey(objectPackSize);
			byte[][] objects = generateRandomObjects(maxObjectSize, objectPackSize, tempID);
			storage.write(tempID, objects);
			System.arraycopy(tempID, 0, ID, i, objectPackSize);

			writeObjectsStoredMessage(i + objectPackSize);
		}
		writeFinishStoringObjectsMessage();

		long fullReadingTime = 0;
		for (int i = 0; i < totalObjectCount; i += objectPackSize) {
			long[] randomID = generateRandomID(objectPackSize, ID);

			long startTime = System.nanoTime();
			byte[][] objects = storage.get(randomID);
			fullReadingTime += System.nanoTime() - startTime;

			for (int j = 0; j < objectPackSize; j++) {
				checkObject(objects[j], randomID[j]);
			}

			writeObjectsCheckedMessage(i + objectPackSize);
		}
		writeFinishCheckingObjectsMessage();
		writeFullAndAverageTimeObjectsReading(totalObjectCount, fullReadingTime);

		//0 1 2 3 4 5
		//0 2 4 6 8 10
		//2 4 6 8 10 12
		int removeObjectsCount = 50_000;
		Set<Long> deletedID = new HashSet<>(removeObjectsCount);
		for (int i = 0; i < removeObjectsCount; i += objectPackSize) {
			// Generating random ID's only from part of ID array - to prevent collisions
			long[] IDpart = Arrays.copyOfRange(ID, i * 2, (i + objectPackSize) * 2);
			long[] randomID = generateDifferentRandomID(objectPackSize, IDpart);
			storage.remove(randomID);

			for (int j = 0; j < objectPackSize; j++) {
				assertNull(storage.get(randomID[j]));
				deletedID.add(randomID[j]);
			}

			writeObjectsRemovedMessage(i + objectPackSize);
		}
		writeFinishRemoveObjectsMessage(removeObjectsCount);

		writeCheckingRemainingObjectsMessage();
		int remainingObjectsCount = totalObjectCount - removeObjectsCount;
		long[] remainingID = new long[remainingObjectsCount];
		int pointer = 0;
		for (int i = 0; i < totalObjectCount; i++) {
			if (!deletedID.contains(ID[i])) {
				remainingID[pointer++] = ID[i];
			}
		}
		assertEquals(pointer, remainingObjectsCount);
		for (int i = 0; i < remainingObjectsCount; i += objectPackSize) {
			long[] tempID = new long[objectPackSize];
			System.arraycopy(remainingID, i, tempID, 0, objectPackSize);

			byte[][] objects = storage.get(tempID);

			for (int j = 0; j < objectPackSize; j++) {
				checkObject(objects[j], tempID[j]);
			}

			writeObjectsCheckedMessage(i + objectPackSize);
		}
		writeFinishCheckingObjectsMessage();

		writeEndMessage();
	}

	private static final int KEY_SIZE = TypeSizes.BYTES_IN_LONG;
	private static final int MD5_HASH_SIZE = 16;

	private static final int KEY_OFFSET = 0;
	private static final int HASH_OFFSET = KEY_OFFSET + KEY_SIZE;
	private static final int OBJECT_OFFSET = HASH_OFFSET + 16;

	public byte[][] generateRandomObjects(int maxObjectSize, int count, long[] ID) throws Exception {
		byte[][] result = new byte[count][];
		for (int i = 0; i < count; i++) {
			result[i] = generateObject(rnd.nextInt(maxObjectSize) + 1, ID[i]);
		}
		return result;
	}

	public byte[] generateObject(int mainSize, long key) throws Exception {
		byte[] mainBytes = new byte[mainSize];
		rnd.nextBytes(mainBytes);
		byte[] hashBytes = MD5(mainBytes); // 16 bytes
		ByteBuffer buffer = ByteBuffer.allocate(TypeSizes.BYTES_IN_LONG);
		byte[] keyBytes = buffer.putLong(key).array(); // 8 bytes

		byte[] result = new byte[mainBytes.length + hashBytes.length + keyBytes.length];
		System.arraycopy(keyBytes, 0, result, KEY_OFFSET, KEY_SIZE);
		System.arraycopy(hashBytes, 0, result, HASH_OFFSET, MD5_HASH_SIZE);
		System.arraycopy(mainBytes, 0, result, OBJECT_OFFSET, mainBytes.length);

		return result;
	}

	public long[] generateRandomID(int count, long[] ID) {
		long[] result = new long[count];

		for (int i = 0; i < count; i++) {
			result[i] = ID[rnd.nextInt(ID.length)];
		}

		return result;
	}

	public long[] generateDifferentRandomID(int count, long[] ID) {
		long[] result = new long[count];

		List<Integer> temp = new ArrayList<>();
		for (int i = 0; i < ID.length; i++) {
			temp.add(i);
		}
		Collections.shuffle(temp, rnd);

		for (int i = 0; i < count; i++) {
			result[i] = ID[temp.get(i)];
		}

		return result;
	}

	private void checkObject(byte[] object, long expectedKey) throws Exception {
		byte[] actualKeyBytes = new byte[KEY_SIZE];
		byte[] expectedHashBytes = new byte[MD5_HASH_SIZE];
		int objectSize = calculateObjectSize(object.length);
		byte[] objectBytes = new byte[objectSize];

		System.arraycopy(object, KEY_OFFSET, actualKeyBytes, 0, KEY_SIZE);
		System.arraycopy(object, HASH_OFFSET, expectedHashBytes, 0, MD5_HASH_SIZE);
		System.arraycopy(object, OBJECT_OFFSET, objectBytes, 0, objectSize);

		long actualKey = ByteBuffer.wrap(actualKeyBytes).getLong();
		assertEquals(expectedKey, actualKey);
		byte[] actualHashBytes = MD5(objectBytes);
		assertArrayEquals(expectedHashBytes, actualHashBytes);
	}

	private int calculateObjectSize(int fullSize) {
		return fullSize - KEY_SIZE - MD5_HASH_SIZE;
	}

	private static byte[] MD5(byte[] bytes) throws NoSuchAlgorithmException {
		return MessageDigest.getInstance("MD5").digest(bytes);
	}

	private void writeStartMessage() throws Exception {
		out.write("Starting big test");
		out.newLine();
		out.newLine();
		out.flush();
	}

	private void writeObjectsStoredMessage(int count) throws Exception {
		out.write("Objects stored: " + count);
		out.newLine();
		out.flush();
	}

	private void writeFinishStoringObjectsMessage() throws Exception {
		out.write("All objects successfully stored");
		out.newLine();
		out.newLine();
		out.flush();
	}

	private void writeObjectsCheckedMessage(int count) throws Exception {
		out.write("Objects checked: " + count);
		out.newLine();
		out.flush();
	}

	private void writeFinishCheckingObjectsMessage() throws Exception {
		out.write("All objects successfully checked");
		out.newLine();
		out.newLine();
		out.flush();
	}

	private void writeFullAndAverageTimeObjectsReading(int objectsCount, long fullNanoTime) throws Exception {
		out.write("Full objects reading time: " + fullNanoTime + " ns (" + (fullNanoTime / 1000) + " ms)");
		out.newLine();
		long averageNanoTime = fullNanoTime / objectsCount;
		out.write("Average object reading time: " + averageNanoTime + " ns (" + (averageNanoTime / 1000) + " ms)");
		out.newLine();
		out.newLine();
		out.flush();
	}

	private void writeObjectsRemovedMessage(int count) throws Exception {
		out.write("Objects removed: " + count);
		out.newLine();
		out.flush();
	}

	private void writeFinishRemoveObjectsMessage(int count) throws Exception {
		out.write(count + " objects successfully removed");
		out.newLine();
		out.newLine();
		out.flush();
	}

	private void writeCheckingRemainingObjectsMessage() throws Exception {
		out.write("Starting check of remaining objects");
		out.newLine();
		out.flush();
	}

	private void writeEndMessage() throws Exception {
		out.write("Test successfully finished");
		out.newLine();
		out.flush();
	}
}
