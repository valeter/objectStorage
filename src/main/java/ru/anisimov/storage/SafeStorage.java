package ru.anisimov.storage;

import ru.anisimov.storage.exceptions.StorageException;
import ru.anisimov.storage.io.FileReaderWriter;

import java.io.File;
import java.io.IOException;

/**
 * @author Ivan Anisimov (ivananisimov2010@gmail.com)
 *
 * Decorator class for Storage object, stores state of operations in separate file.
 * On initialization checks - if all operations finished well.
 *
 */
public class SafeStorage implements Storage {
	private static final byte STABLE_STATE = 1;
	private static final byte UNSTABLE_STATE = -1;

	private Storage storage;
	private String safetyFileName;

	public SafeStorage(Storage storage, String safetyFileName, boolean newStorage) throws StorageException {
		this.safetyFileName = safetyFileName;
		this.storage = storage;
		try (FileReaderWriter out = FileReaderWriter.openForWriting(safetyFileName)) {
			if (newStorage) {
				File file = new File(this.safetyFileName);
				if (file.exists()) {
					file.delete();
				}
				file.createNewFile();
				setState(out, STABLE_STATE);
			}
			if (getState() == UNSTABLE_STATE) {
				storage.rebuild();
			}
		} catch (IOException e) {
			throw new StorageException(e);
		}
	}

	private void setState(FileReaderWriter out, byte state) throws IOException {
		out.writeBytes(0, state);
	}

	private byte getState() throws IOException {
		try (FileReaderWriter in = FileReaderWriter.openForReading(safetyFileName)) {
			return in.readByte(0);
		}
	}

	@Override
	public long write(final byte[] bytes) throws StorageException {
		return safeOperation(new StorageOperation<Long>() {
			@Override
			public Long perform() throws StorageException {
				return storage.write(bytes);
			}
		});
	}

	@Override
	public long[] write(final byte[][] bytes) throws StorageException {
		return safeOperation(new StorageOperation<long[]>() {
			@Override
			public long[] perform() throws StorageException {
				return storage.write(bytes);
			}
		});
	}

	@Override
	public byte[] get(final long key) throws StorageException {
		return safeOperation(new StorageOperation<byte[]>() {
			@Override
			public byte[] perform() throws StorageException {
				return storage.get(key);
			}
		});
	}

	@Override
	public byte[][] get(final long[] keys) throws StorageException {
		return safeOperation(new StorageOperation<byte[][]>() {
			@Override
			public byte[][] perform() throws StorageException {
				return storage.get(keys);
			}
		});
	}

	@Override
	public boolean remove(final long key) throws StorageException {
		return safeOperation(new StorageOperation<Boolean>() {
			@Override
			public Boolean perform() throws StorageException {
				return storage.remove(key);
			}
		});
	}

	@Override
	public boolean remove(final long[] keys) throws StorageException {
		return safeOperation(new StorageOperation<Boolean>() {
			@Override
			public Boolean perform() throws StorageException {
				return storage.remove(keys);
			}
		});
	}

	@Override
	public RebuildInfo rebuild() throws StorageException {
		return safeOperation(new StorageOperation<RebuildInfo>() {
			@Override
			public RebuildInfo perform() throws StorageException {
				return storage.rebuild();
			}
		});
	}

	@Override
	public long getMaxObjectSize() {
		return storage.getMaxObjectSize();
	}

	private <T> T safeOperation(StorageOperation<T> operation) throws StorageException {
		try (FileReaderWriter out = FileReaderWriter.openForWriting(safetyFileName)) {
			setState(out, UNSTABLE_STATE);
			T result = operation.perform();
			setState(out, STABLE_STATE);
			return result;
		} catch (IOException e) {
			throw new StorageException(e);
		}
	}

	private interface StorageOperation<T> {
		T perform() throws StorageException;
	}
}
