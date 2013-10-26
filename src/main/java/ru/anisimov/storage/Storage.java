package ru.anisimov.storage;

/**
 * @author Ivan Anisimov (ivananisimov2010@gmail.com)
 */
public interface Storage {
	long generateKey();
	boolean write(long key, byte[] bytes);
	byte[] get(long key);
	boolean remove(long key);
}
