package ru.anisimov.storage.exceptions;

/**
 * @author Ivan Anisimov (ivananisimov2010@gmail.com)
 */
public class IDGeneratorException extends Exception {
	private static final long serialVersionUID = 5043943178966938161L;

	public IDGeneratorException() {
		super();
	}

	public IDGeneratorException(String message) {
		super(message);
	}

	public IDGeneratorException(String message, Throwable cause) {
		super(message, cause);
	}

	public IDGeneratorException(Throwable cause) {
		super(cause);
	}

	public IDGeneratorException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
