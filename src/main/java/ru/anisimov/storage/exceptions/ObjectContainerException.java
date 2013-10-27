package ru.anisimov.storage.exceptions;

/**
 * @author Ivan Anisimov (ivananisimov2010@gmail.com)
 */
public class ObjectContainerException extends Exception {
	public ObjectContainerException() {
		super();
	}

	public ObjectContainerException(String message) {
		super(message);
	}

	public ObjectContainerException(String message, Throwable cause) {
		super(message, cause);
	}

	public ObjectContainerException(Throwable cause) {
		super(cause);
	}

	public ObjectContainerException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
