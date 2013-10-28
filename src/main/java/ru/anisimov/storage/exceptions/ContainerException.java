package ru.anisimov.storage.exceptions;

/**
 * @author Ivan Anisimov (ivananisimov2010@gmail.com)
 */
public class ContainerException extends Exception {
	public ContainerException() {
		super();
	}

	public ContainerException(String message) {
		super(message);
	}

	public ContainerException(String message, Throwable cause) {
		super(message, cause);
	}

	public ContainerException(Throwable cause) {
		super(cause);
	}
}
