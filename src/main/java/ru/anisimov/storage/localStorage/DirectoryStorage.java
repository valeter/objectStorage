package ru.anisimov.storage.localStorage;

import ru.anisimov.storage.RebuildInfo;
import ru.anisimov.storage.SafeStorage;
import ru.anisimov.storage.Storage;
import ru.anisimov.storage.exceptions.ContainerException;
import ru.anisimov.storage.exceptions.IDGeneratorException;
import ru.anisimov.storage.exceptions.StorageException;
import ru.anisimov.storage.io.FileReaderWriter;

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
	private static final String NULL_OR_NOT_SAME_ARRAY_MESSAGE = "Input arrays is null or not same size";

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

	private static void checkDirectoryName(String directoryName) throws NoSuchFileException, NotDirectoryException {
		File file = new File(directoryName);
		if (!file.exists()) {
			throw new NoSuchFileException("No such file or directory: " + directoryName);
		}
		if (!file.isDirectory()) {
			throw new NotDirectoryException("Specified path is not a directory: " + directoryName);
		}
	}

	public static Storage newStorage(String directoryName) throws StorageException {
		return new SafeStorage(new DirectoryStorage(directoryName, true), directoryName + SLASH + SAFETY_FILE_NAME, true);
	}

	public static Storage getStorage(String directoryName) throws StorageException {
		return new SafeStorage(new DirectoryStorage(directoryName, false), directoryName + SLASH + SAFETY_FILE_NAME, false);
	}

	@Override
	public long generateKey() throws StorageException {
		return generateKey(1)[0];
	}

	@Override
	public long[] generateKey(int count) throws StorageException {
		try {
			return generator.generateID(count);
		} catch (IDGeneratorException e) {
			throw new StorageException(e);
		}
	}

	@Override
	public long write(byte[] bytes) throws StorageException {
		return write(new byte[][] {bytes})[0];
	}

	@Override
	public long[] write(byte[][] bytes) throws StorageException {
		if (bytes == null) {
			throw new StorageException(NULL_ARRAY_MESSAGE);
		}
		checkBytes(bytes);
		try {
			long[] keys = generateKey(bytes.length);
			ObjectAddress[] addresses = container.put(keys, bytes);
			index.putAddress(keys, addresses);
			return keys;
		} catch (Exception e) {
			throw new StorageException(e);
		}
	}

	private void checkBytes(byte[][] bytes) throws StorageException {
		for (int i = 0; i < bytes.length; i++) {
			if (bytes[i] == null) {
				throw new StorageException(NULL_ARRAY_MESSAGE);
			}
		}
	}

	@Override
	public boolean write(long key, byte[] bytes) throws StorageException {
		return write(new long[] {key}, new byte[][] {bytes});
	}

	@Override
	public boolean write(long[] keys, byte[][] bytes) throws StorageException {
		if (bytes == null || keys == null || bytes.length != keys.length) {
			throw new StorageException(NULL_OR_NOT_SAME_ARRAY_MESSAGE);
		}
		checkBytes(bytes);
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
			RecordData[] data = container.get(addresses);
			for (int i = 0; i < resultLength; i++) {
				result[i] = (data[i] == null) ? null : data[i].getObject();
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
			generator.addFreeID(keys);
			index.removeAddress(keys);
			container.remove(addresses);
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
			String pathStart = directoryName + System.getProperty("file.separator");
			for (String fileName: files) {
					int num;
					try {
						num = Integer.parseInt(fileName.substring(CONTAINER_FILE_PREFIX.length()));
					} catch (NumberFormatException e) {
						continue;
					}

					String fullPath = new StringBuilder().append(pathStart).append(fileName).toString();
					String tempFileName = new StringBuilder().append(fullPath).append(".temp").toString();
					Files.copy(new File(fullPath).toPath(), new File(tempFileName).toPath(), StandardCopyOption.REPLACE_EXISTING);

					getDataFromContainer(num, tempFileName, resultBuilder);

					System.gc(); // Attempt to remove FileChannel.map blocks from files
					new File(tempFileName).delete();
			}
			clearTempFiles();
		} catch (IOException | ContainerException e) {
			throw new StorageException(e);
		}

		return resultBuilder.build();
	}

	private void getDataFromContainer(int containerNumber, String containerFileName, DirectoryStorageRebuildInfo.Builder resultBuilder) throws StorageException {
		try (FileReaderWriter rw = FileReaderWriter.openForReadingWriting(containerFileName)) {
			ObjectContainer tempContainer = new ObjectContainer(rw, containerFileName, containerNumber, false);
			List<ObjectAddress> addresses = tempContainer.getRecordsAddresses(rw);
			for (ObjectAddress address : addresses) {
				RecordData data = tempContainer.getData(rw, address.getFilePosition());
				ObjectAddress newAddress = container.put(data.getID(), data.getObject());
				index.putAddress(data.getID(), newAddress);
			}
		} catch (Exception e) {
			resultBuilder.addLostContainer(containerFileName);
		}
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
