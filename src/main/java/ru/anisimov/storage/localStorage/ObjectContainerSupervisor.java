package ru.anisimov.storage.localStorage;

import ru.anisimov.storage.commons.TypeSizes;
import ru.anisimov.storage.exceptions.ObjectContainerException;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @author Ivan Anisimov (ivananisimov2010@gmail.com)
 *
 * Manages ObjectContainers. Checkes size of incoming files.
 * Creates new ObjectContainers if necessary.
 *
 */
public class ObjectContainerSupervisor {
	private static final int ESTIMATED_MAX_CONTAINER_COUNT = 1000;
	private static final long ESTIMATED_MAX_FILE_SIZE = 2^31;
	private static final double CONTAINER_FILL_RATIO = 0.9;

	private final long MAX_FILE_SIZE;
	private final long FULL_CONTAINER_SIZE;

	private ArrayList<ObjectContainer> containers;
	private ArrayList<ObjectContainer> freeContainers;

	private String directoryName;

	public ObjectContainerSupervisor(String directoryName) {
		this(directoryName, ESTIMATED_MAX_FILE_SIZE);
	}

	protected ObjectContainerSupervisor(String directoryName, long MAX_FILE_SIZE) {
		this.MAX_FILE_SIZE = MAX_FILE_SIZE;
		this.FULL_CONTAINER_SIZE = (long)(this.MAX_FILE_SIZE * CONTAINER_FILL_RATIO);
		this.directoryName = directoryName;
		containers = new ArrayList<>(ESTIMATED_MAX_CONTAINER_COUNT);
		freeContainers = new ArrayList<>(ESTIMATED_MAX_CONTAINER_COUNT);
	}

	public long getMaxObjectSize(int objectsCount) {
		return ((MAX_FILE_SIZE - TypeSizes.BYTES_IN_LONG) / objectsCount) - (ObjectContainer.getNeededSpace(new byte[0]) * objectsCount);
	}

	protected int getContainersCount() {
		return containers.size();
	}

	public void remove(ObjectAddress address) throws IOException {
		containers.get(address.getFileNumber()).removeBytes(address.getFilePosition());
	}

	public ObjectAddress put(int ID, byte[] bytes) throws IOException, ObjectContainerException {
		if (ObjectContainer.getNeededSpace(bytes) > MAX_FILE_SIZE) {
			throw new ObjectContainerException("Too big object");
		}

		ObjectContainer bestContainer = getBestContainer(bytes);
		if (bestContainer == null) { //|| bestContainer.getSize() + ObjectContainer.getNeededSpace(bytes) > MAX_FILE_SIZE) {
			ObjectContainer container =
					new ObjectContainer(directoryName + System.getProperty("file.separator") + containers.size(),
											   containers.size(), true);
			containers.add(container);
			freeContainers.add(container);
			bestContainer = container;
		}
		ObjectAddress result = bestContainer.writeBytes(ID, bytes);
		if (bestContainer.getSize() >= FULL_CONTAINER_SIZE) {
			freeContainers.remove(bestContainer);
		}

		return result;
	}

	private ObjectContainer getBestContainer(byte[] bytes) {
		ObjectContainer result = null;
		long bestQuality = Long.MAX_VALUE;
		for (ObjectContainer container : freeContainers) {
			long quality = MAX_FILE_SIZE - container.getSize() - ObjectContainer.getNeededSpace(bytes);
			if (quality >= 0 && quality < bestQuality) {
				bestQuality = quality;
				result = container;
			}
		}
		return result;
	}

	public ObjectContainer.RecordData get(ObjectAddress address) throws IOException {
		return containers.get(address.getFileNumber()).getData(address.getFilePosition());
	}
}
