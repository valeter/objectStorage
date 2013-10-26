package ru.anisimov.storage;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Ivan Anisimov (ivananisimov2010@gmail.com)
 */
public class LocalStorage implements Storage {
	private static Map<String, LocalStorage> instances = new HashMap<>();

	private String directoryName;

	private LocalStorage(String directoryName) {
		this.directoryName = directoryName;
	}

	// Controls condition that in one directory can work only one LocalStorage
	// TODO: Decide - do you really need this?
	public static LocalStorage getInstance(String directoryName) throws IOException {
		checkDirectoryName(directoryName);

		String key = new File(directoryName).getCanonicalPath();
		if (!instances.containsKey(key)) {
			instances.put(key, new LocalStorage(key));
		}
		return instances.get(key);
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
	public long generateKey() {
		return 0;  //To change body of implemented methods use File | Settings | File Templates.
	}

	@Override
	public boolean write(long key, byte[] bytes) {
		return false;  //To change body of implemented methods use File | Settings | File Templates.
	}

	@Override
	public byte[] get(long key) {
		return new byte[0];  //To change body of implemented methods use File | Settings | File Templates.
	}

	@Override
	public boolean remove(long key) {
		return false;  //To change body of implemented methods use File | Settings | File Templates.
	}
}
