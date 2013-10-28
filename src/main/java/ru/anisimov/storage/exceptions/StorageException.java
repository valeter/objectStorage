package ru.anisimov.storage.exceptions;

/**
 * @author Ivan Anisimov (ivananisimov2010@gmail.com)
 */
public class StorageException extends Exception {
	public StorageException() {
		super();
	}

	public StorageException(String message) {
		super(message);
	}

	public StorageException(String message, Throwable cause) {
		super(message, cause);
	}

	public StorageException(Throwable cause) {
		super(cause);
	}
}
