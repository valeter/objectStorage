package ru.anisimov.storage.commons;

import java.util.Random;

/**
 * @author Ivan Anisimov (ivananisimov2010@gmail.com)
 */
public class DataGenerator {
	private static final Random rnd = new Random(System.currentTimeMillis());

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

	public static long[] generateDifferentLongs(int count) {
		long[] result = new long[count];

		long[] temp = new long[count];
		for (int i = 0; i < count; i++) {
			temp[i] = i;
		}

		int top = temp.length;
		for (int i = 0; i < count; i++) {
			int nextInd = rnd.nextInt(top--);
			result[i] = temp[nextInd];
			long t = temp[nextInd];
			temp[nextInd] = temp[top];
			temp[top] = t;
		}

		return result;
	}

	public static <T> void generateObjects(T[] array, ObjectGenerator<T> generator) {
		for (int i = 0; i < array.length; i++) {
			array[i] = generator.generate(rnd);
		}
	}

	public static interface ObjectGenerator<T> {
		T generate(Random rnd);
	}
}
