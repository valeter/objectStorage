package ru.anisimov.storage;

import ru.anisimov.storage.exceptions.StorageException;

/**
 * @author Ivan Anisimov (ivananisimov2010@gmail.com)
 */
public interface Storage {
	long write(byte[] bytes) throws StorageException;
	long[] write(byte[][] bytes) throws StorageException;
	byte[] get(long key) throws StorageException;
	byte[][] get(long[] keys) throws StorageException;
	boolean remove(long key) throws StorageException;
	boolean remove(long[] keys) throws StorageException;
	RebuildInfo rebuild() throws StorageException;
	long getMaxObjectSize();
}
