package ru.anisimov.storage.exceptions;

/**
 * @author Ivan Anisimov (ivananisimov2010@gmail.com)
 */
public class IndexException extends Exception {
	public IndexException() {
		super();
	}

	public IndexException(String message) {
		super(message);
	}

	public IndexException(String message, Throwable cause) {
		super(message, cause);
	}

	public IndexException(Throwable cause) {
		super(cause);
	}
}
