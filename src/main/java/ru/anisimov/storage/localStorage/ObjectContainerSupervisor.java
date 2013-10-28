package ru.anisimov.storage.localStorage;

import ru.anisimov.storage.commons.TypeSizes;
import ru.anisimov.storage.exceptions.ContainerException;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.*;

/**
 * @author Ivan Anisimov (ivananisimov2010@gmail.com)
 *
 * Manages ObjectContainers. Checkes size of incoming files.
 * Creates new ObjectContainers if necessary.
 *
 * For writing always choose container which fits best for current object
 * (MAX_FILE_SIZE - container.getSize() - ObjectContainer.getNeededSpace(bytes) => min)
 *
 */
public class ObjectContainerSupervisor {
	private static final int ESTIMATED_MAX_CONTAINER_COUNT = 1000;
	private static final long ESTIMATED_MAX_FILE_SIZE = Integer.MAX_VALUE;
	private static final double CONTAINER_FILL_RATIO = 0.9;

	private final long MAX_FILE_SIZE;
	private final long FULL_CONTAINER_SIZE;
	private final String CONTAINER_FILE_NAME_PREFIX;

	private List<ObjectContainer> containers;
	private List<ObjectContainer> freeContainers;

	private String directoryName;

	public ObjectContainerSupervisor(String directoryName, String CONTAINER_FILE_NAME_PREFIX, boolean newSupervisor) throws ContainerException {
		this(directoryName, CONTAINER_FILE_NAME_PREFIX, newSupervisor, ESTIMATED_MAX_FILE_SIZE);
	}

	protected ObjectContainerSupervisor(String directoryName, String CONTAINER_FILE_NAME_PREFIX, boolean newSupervisor, long MAX_FILE_SIZE) throws ContainerException {
		this.CONTAINER_FILE_NAME_PREFIX = CONTAINER_FILE_NAME_PREFIX;
		this.MAX_FILE_SIZE = MAX_FILE_SIZE;
		this.FULL_CONTAINER_SIZE = (long)(this.MAX_FILE_SIZE * CONTAINER_FILL_RATIO);
		this.directoryName = directoryName;
		containers = new ArrayList<>(ESTIMATED_MAX_CONTAINER_COUNT);
		freeContainers = new ArrayList<>(ESTIMATED_MAX_CONTAINER_COUNT);
		if (!newSupervisor) {
			try {
				findContainers();
			} catch (Exception e) {
				throw new ContainerException(e);
			}
		}
	}

	private boolean isFree(ObjectContainer container) {
		return container.getSize() < FULL_CONTAINER_SIZE;
	}

	private void findContainers() throws IOException, ContainerException {
		String[] files = new File(directoryName).list(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.startsWith(CONTAINER_FILE_NAME_PREFIX);
			}
		});

		for (String fileName: files) {
			String fullPath = directoryName + System.getProperty("file.separator") + fileName;
			int num;
			try {
				num = Integer.parseInt(fileName.substring(CONTAINER_FILE_NAME_PREFIX.length()));
			} catch (NumberFormatException e) {
				continue;
			}
			ObjectContainer container = new ObjectContainer(fullPath, num, false);

			containers.add(container);
			if (isFree(container)) {
				freeContainers.add(container);
			}
		}

		Collections.sort(containers, new Comparator<ObjectContainer>() {
			@Override
			public int compare(ObjectContainer o1, ObjectContainer o2) {
				return Integer.compare(o1.getNumber(), o2.getNumber());
			}
		});

		for (int i = 0; i < containers.size(); i++) {
			ObjectContainer container = containers.get(i);
			if (container.getNumber() != i) {
				throw new ContainerException("Lost container with number: " + i);
			}
		}
	}

	public long getMaxObjectSize(int objectsCount) {
		return ((MAX_FILE_SIZE - TypeSizes.BYTES_IN_LONG) / objectsCount) - (ObjectContainer.getNeededSpace(new byte[0]) * objectsCount);
	}

	protected int getContainersCount() {
		return containers.size();
	}

	public void remove(ObjectAddress address) throws ContainerException {
		remove(new ObjectAddress[] {address});
	}

	public void remove(ObjectAddress[] addresses) throws ContainerException {
		try {
			Map<Integer, List<ObjectAddress>> addressesByContainer = spreadByContainerNumber(addresses);
			for (Integer containerIndex : addressesByContainer.keySet()) {
				List<ObjectAddress> containerAddresses = addressesByContainer.get(containerIndex);
				long[] positions = getPositionsFromAddressList(containerAddresses);
				containers.get(containerIndex).removeBytes(positions);
			}
		} catch (Exception e) {
			throw new ContainerException(e);
		}
	}

	public ObjectAddress put(long ID, byte[] bytes) throws ContainerException {
		return put(new long[] {ID}, new byte[][] {bytes})[0];
	}

	// Container packaging task is NP-complex, so, I don't think that it should be solved here
	// Objects packs with primitive algorithm
	public ObjectAddress[] put(long[] ID, byte[][] bytes) throws ContainerException {
		int objectsCount = ID.length;
		for (int i = 0; i < objectsCount; i++) {
			if (ObjectContainer.getNeededSpace(bytes[i]) > MAX_FILE_SIZE) {
				throw new ContainerException("Too big object");
			}
		}

		int startObject = 0;
		int curCount = 0;
		try {
			ObjectAddress[] result = new ObjectAddress[objectsCount];
			int pointer = 0;
			ObjectContainer lastContainer = null;
			while (startObject < objectsCount) {
				long sumSize = 0;
				while (startObject + curCount < bytes.length) {
					long neededSize = ObjectContainer.getNeededSpace(bytes[startObject + curCount]);
					if (sumSize + neededSize > MAX_FILE_SIZE) {
						break;
					}
					sumSize += neededSize;
					curCount++;
				}
				if (curCount <= 0) {
					throw new ContainerException("Could not write objects to container");
				}

				ObjectContainer container =
						new ObjectContainer(directoryName + System.getProperty("file.separator") +
													CONTAINER_FILE_NAME_PREFIX + containers.size(), containers.size(), true);
				containers.add(container);
				lastContainer = container;

				ObjectAddress[] subResult = container.writeBytes(ID, bytes, startObject, curCount);
				System.arraycopy(subResult, 0, result, pointer, subResult.length);
				pointer += subResult.length;
				startObject += curCount;
				curCount = 0;
			}

			if (isFree(lastContainer)) {
				freeContainers.add(lastContainer);
			}
			return result;
		} catch (IOException e) {
			throw new ContainerException(e);
		}
	}

	/*private ObjectContainer getBestContainer(byte[] bytes) {
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
	}*/

	public RecordData get(ObjectAddress address) throws ContainerException {
		return get(new ObjectAddress[] {address})[0];
	}

	public RecordData[] get(ObjectAddress[] addresses) throws ContainerException {
		try {
			int pointer = 0;
			RecordData[] result = new RecordData[addresses.length];
			Map<Integer, List<ObjectAddress>> addressesByContainer = spreadByContainerNumber(addresses);
			for (Integer containerIndex : addressesByContainer.keySet()) {
				List<ObjectAddress> containerAddresses = addressesByContainer.get(containerIndex);
				long[] positions = getPositionsFromAddressList(containerAddresses);
				RecordData[] subResult = containers.get(containerIndex).getData(positions);
				System.arraycopy(subResult, 0, result, pointer, subResult.length);
				pointer += subResult.length;
				continue;
			}
			return result;
		} catch (Exception e) {
			throw new ContainerException(e);
		}
	}

	private long[] getPositionsFromAddressList(List<ObjectAddress> addresses) {
		long[] result = new long[addresses.size()];
		Iterator<ObjectAddress> iterator = addresses.iterator();
		int pointer = 0;
		while (iterator.hasNext()) {
			result[pointer++] = iterator.next().getFilePosition();
		}
		return result;
	}

	private Map<Integer, List<ObjectAddress>> spreadByContainerNumber(ObjectAddress[] addresses) {
		Map<Integer, List<ObjectAddress>> result = new TreeMap<>();
		for (ObjectAddress address : addresses) {
			int fileNumber = address.getFileNumber();
			if (!result.containsKey(fileNumber)) {
				result.put(fileNumber, new LinkedList<ObjectAddress>());
			}
			result.get(fileNumber).add(address);
		}
		return result;
	}

	public List<ObjectContainer> getContainers() {
		return containers;
	}
}
