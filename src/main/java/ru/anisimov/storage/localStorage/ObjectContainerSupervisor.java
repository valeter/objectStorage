package ru.anisimov.storage.localStorage;

import ru.anisimov.storage.commons.TypeSizes;
import ru.anisimov.storage.exceptions.ObjectContainerException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * @author Ivan Anisimov (ivananisimov2010@gmail.com)
 */
public class ObjectContainerSupervisor {
	private static final int ESTIMATED_MAX_CONTAINER_COUNT = 1000;
	private static final long ESTIMATED_MAX_FILE_SIZE = 2^31;

	private final long MAX_FILE_SIZE;

	private PriorityQueue<ObjectContainer> containersQueue;
	private ArrayList<ObjectContainer> containers;

	private String directoryName;

	public ObjectContainerSupervisor(String directoryName) {
		this(directoryName, ESTIMATED_MAX_FILE_SIZE);
	}

	protected ObjectContainerSupervisor(String directoryName, long MAX_FILE_SIZE) {
		this.MAX_FILE_SIZE = MAX_FILE_SIZE;
		this.directoryName = directoryName;
		containersQueue = new PriorityQueue<>(ESTIMATED_MAX_CONTAINER_COUNT, new Comparator<ObjectContainer>() {
			@Override
			public int compare(ObjectContainer o1, ObjectContainer o2) {
				return Long.compare(o1.getSize(), o2
														  .getSize());
			}
		});
		containers = new ArrayList<>(ESTIMATED_MAX_CONTAINER_COUNT);
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
			throw  new ObjectContainerException("Too big object");
		}

		ObjectContainer smallest = containersQueue.peek();
		if (smallest == null || smallest.getSize() + ObjectContainer.getNeededSpace(bytes) > MAX_FILE_SIZE) {
			ObjectContainer container =
					new ObjectContainer(directoryName + System.getProperty("file.separator") + containers.size(),
											   containers.size(), true);
			containers.add(container);
			containersQueue.add(container);
			smallest = container;

		}

		return smallest.writeBytes(ID, bytes);
	}

	public byte[] get(ObjectAddress address) throws IOException {
		return containers.get(address.getFileNumber()).getBytes(address.getFilePosition());
	}
}
