package ru.anisimov.storage.localStorage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * @author Ivan Anisimov (ivananisimov2010@gmail.com)
 */
public class ObjectContainerSupervisor {
	private static final int ESTIMATED_MAX_CONTAINER_COUNT = 1000;
	private static final long MAX_32_FILE_SIZE = 2^32 - 1;

	private final long MAX_FILE_SIZE;

	private PriorityQueue<ObjectContainer> containersQueue;
	private ArrayList<ObjectContainer> containers;

	private String directoryName;

	public ObjectContainerSupervisor(String directoryName) {
		this(directoryName, MAX_32_FILE_SIZE);
	}

	ObjectContainerSupervisor(String directoryName, long MAX_FILE_SIZE) {
		this.MAX_FILE_SIZE = MAX_FILE_SIZE;
		this.directoryName = directoryName;
		containersQueue = new PriorityQueue<>(ESTIMATED_MAX_CONTAINER_COUNT, new Comparator<ObjectContainer>() {
			@Override
			public int compare(ObjectContainer o1, ObjectContainer o2) {
				return Long.compare(o2.getSize(), o1.getSize());
			}
		});
		containers = new ArrayList<>(ESTIMATED_MAX_CONTAINER_COUNT);
	}

	public void remove(ObjectAddress address) throws IOException {
		containers.get(address.getFileNumber()).removeBytes(address.getFilePosition(), address.getObjectSize());
	}

	public ObjectAddress put(byte[] bytes) throws IOException {
		ObjectContainer smallest = containersQueue.peek();
		if (smallest == null || smallest.getSize() + ObjectContainer.getNeededSpace(bytes)> MAX_FILE_SIZE) {
			ObjectContainer container =
					new ObjectContainer(directoryName + System.getProperty("path.separator") + containers.size(),
											   containers.size(), true);
			containers.add(container);
			containersQueue.add(container);
			smallest = container;
		}

		smallest.writeBytes(bytes);
		return new ObjectAddress(smallest.getNumber(), smallest.getLastByte(), bytes.length);
	}

	public byte[] get(ObjectAddress address) throws IOException {
		return containers.get(address.getFileNumber()).getBytes(address.getFilePosition(),
																	   address.getObjectSize());
	}
}
