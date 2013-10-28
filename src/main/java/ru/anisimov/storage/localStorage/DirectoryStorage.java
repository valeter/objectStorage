package ru.anisimov.storage.localStorage;

import ru.anisimov.storage.RebuildInfo;
import ru.anisimov.storage.SafeStorage;
import ru.anisimov.storage.Storage;
import ru.anisimov.storage.exceptions.ContainerException;
import ru.anisimov.storage.exceptions.IDGeneratorException;
import ru.anisimov.storage.exceptions.StorageException;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.StandardCopyOption;
import java.util.List;

/**
 * @author Ivan Anisimov (ivananisimov2010@gmail.com)
 */
public class DirectoryStorage implements Storage {
	private static final String NULL_ARRAY_MESSAGE = "Input array is null";
	private static final String NULL_OR_NOT_EQUAL_LENGTH_ARRAYS_MESSAGE = "Input arrays are null or their length is not the same";

	private static final String SLASH = System.getProperty("file.separator");

	private static final String GENERATOR_FILE_NAME = "gen";
	private static final String INDEX_FILE_NAME = "ind";
	private static final String SAFETY_FILE_NAME = "safe";
	private static final String CONTAINER_FILE_PREFIX = "cont";

	private FileBasedIDGenerator generator;
	private FileBasedIndex index;
	private ObjectContainerSupervisor container;

	private String directoryName;

	private DirectoryStorage(String directoryName, boolean newStorage) throws StorageException {
		try {
			checkDirectoryName(directoryName);
			this.directoryName = directoryName;

			generator = new FileBasedIDGenerator(directoryName + SLASH + GENERATOR_FILE_NAME, newStorage);
			index = new FileBasedIndex(directoryName + SLASH + INDEX_FILE_NAME, newStorage);
			container = new ObjectContainerSupervisor(directoryName, CONTAINER_FILE_PREFIX, newStorage);
		} catch (IOException | ContainerException e) {
			throw new StorageException(e);
		}
	}

	public static Storage newStorage(String directoryName) throws StorageException {
		return new SafeStorage(new DirectoryStorage(directoryName, true), directoryName + SLASH + SAFETY_FILE_NAME);
	}

	public static Storage getStorage(String directoryName) throws StorageException {
		return new SafeStorage(new DirectoryStorage(directoryName, false), directoryName + SLASH + SAFETY_FILE_NAME);
	}

	private static void checkDirectoryName(String directoryName) throws NoSuchFileException, NotDirectoryException {
		File file = new File(directoryName);
		if (!file.exists()) {
			throw new NoSuchFileException("No such file or directory: " + directoryName);
		}
		if (!file.isDirectory()) {
			throw new NotDirectoryException("Specified path is not a directory: " + directoryName);
		}
	}

	@Override
	public long generateKey() throws StorageException {
		try {
			return generator.generateID();
		} catch (IDGeneratorException e) {
			throw new StorageException(e);
		}
	}

	@Override
	public boolean write(long key, byte[] bytes) throws StorageException {
		return write(new long[] {key}, new byte[][] {bytes});
	}

	@Override
	public boolean write(long[] keys, byte[][] bytes) throws StorageException {
		if (keys == null || bytes == null || keys.length != bytes.length) {
			throw new StorageException(NULL_OR_NOT_EQUAL_LENGTH_ARRAYS_MESSAGE);
		}
		for (int i = 0; i < bytes.length; i++) {
			if (bytes[i] == null) {
				throw new StorageException(NULL_ARRAY_MESSAGE);
			}
		}
		try {
			ObjectAddress[] addresses = container.put(keys, bytes);
			index.putAddress(keys, addresses);
			return true;
		} catch (Exception e) {
			throw new StorageException(e);
		}
	}

	@Override
	public byte[] get(long key) throws StorageException {
		return get(new long[] {key})[0];
	}

	@Override
	public byte[][] get(long[] keys) throws StorageException {
		if (keys == null) {
			throw new StorageException(NULL_ARRAY_MESSAGE);
		}
		try {
			int resultLength = keys.length;
			byte[][] result = new byte[resultLength][];
			ObjectAddress[] addresses = index.getAddress(keys);
			for (int i = 0; i < resultLength; i++) {
				if (addresses[i] == null || addresses[i] == ObjectAddress.EMPTY_ADDRESS) {
					result[i] = null;
					continue;
				}
				RecordData data = container.get(addresses[i]);
				result[i] = (data == null) ? null : data.getObject();
			}

			return result;
		} catch (Exception e) {
			throw new StorageException(e);
		}
	}

	@Override
	public boolean remove(long key) throws StorageException {
		return remove(new long[] {key});
	}

	@Override
	public boolean remove(long[] keys) throws StorageException {
		if (keys == null) {
			throw new StorageException(NULL_ARRAY_MESSAGE);
		}
		try {
			ObjectAddress[] addresses = index.getAddress(keys);
			for (int i = 0; i < addresses.length; i++) {
				ObjectAddress address = addresses[i];
				if (address == null || address == ObjectAddress.EMPTY_ADDRESS) {
					continue;
				}
				generator.addFreeID(keys);
				index.removeAddress(keys);
				container.remove(address);
			}
			return true;
		} catch (Exception e) {
			throw new StorageException(e);
		}
	}

	@Override
	public RebuildInfo rebuild() throws StorageException {
		DirectoryStorageRebuildInfo.Builder resultBuilder = new DirectoryStorageRebuildInfo.Builder();

		try {
			container = new ObjectContainerSupervisor(directoryName, CONTAINER_FILE_PREFIX, true);
			index = new FileBasedIndex(directoryName + SLASH + INDEX_FILE_NAME, true);
			String[] files = new File(directoryName).list(new FilenameFilter() {
				@Override
				public boolean accept(File dir, String name) {
					return name.startsWith(CONTAINER_FILE_PREFIX);
				}
			});
			for (String fileName: files) {
				try {
					String fullPath = directoryName + System.getProperty("file.separator") + fileName;
					int num;
					try {
						num = Integer.parseInt(fileName.substring(CONTAINER_FILE_PREFIX.length()));
					} catch (NumberFormatException e) {
						continue;
					}

					String tempFileName = fullPath + ".temp";
					Files.copy(new File(fullPath).toPath(), new File(tempFileName).toPath(), StandardCopyOption.REPLACE_EXISTING);
					ObjectContainer tempContainer = new ObjectContainer(tempFileName, num, false);
					List<ObjectAddress> addresses = tempContainer.getRecordsAddresses();
					for (ObjectAddress address : addresses) {
						RecordData data = tempContainer.getData(address.getFilePosition());
						ObjectAddress newAddress = container.put(data.getID(), data.getObject());
						index.putAddress(data.getID(), newAddress);
					}
					System.gc(); // Attempt to remove FileChannel.map blocks from files
					new File(tempFileName).delete();
				} catch (IOException e) {
					resultBuilder.addLostContainer(fileName);
				} catch (Exception e) {
					throw new StorageException(e);
				}
			}
			clearTempFiles();
		} catch (Exception e) {
			throw new StorageException(e);
		}

		return resultBuilder.build();
	}

	private void clearTempFiles() {
		System.gc(); // Attempt to remove FileChannel.map blocks from files
		String[] files = new File(directoryName).list(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".temp");
			}
		});
		for (String f : files) {
			new File(directoryName + System.getProperty("file.separator") + f).delete();
		}
	}

	@Override
	public long getMaxObjectSize() {
		try {
			return container.getMaxObjectSize(1);
		} catch (Exception e) {
			return 0;
		}
	}
}
