package ru.anisimov.storage.localStorage;

import ru.anisimov.storage.commons.TypeSizes;
import ru.anisimov.storage.exceptions.ContainerException;
import ru.anisimov.storage.io.FileReaderWriter;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * @author Ivan Anisimov (ivananisimov2010@gmail.com)
 *
 * Manages ObjectContainers. Checkes size of incoming files.
 * Creates new ObjectContainers if necessary.
 *
 * Uses primitive grouping algorythm if many objects added.
 *
 */
public class ObjectContainerSupervisor {
	private static final long ESTIMATED_MAX_FILE_SIZE = Integer.MAX_VALUE;
	private static final String SUPERVISOR_INFO_FILE_NAME = "supervisorInfo";

	private final long MAX_FILE_SIZE;
	private final String CONTAINER_FILE_NAME_PREFIX;
	private final String CONTAINER_PATH_START;
	private final String SUPERVISOR_INFO_FILE_PATH;

	private String directoryName;
	private int nextContainerNumber;

	public ObjectContainerSupervisor(String directoryName, String CONTAINER_FILE_NAME_PREFIX, boolean newSupervisor) throws ContainerException {
		this(directoryName, CONTAINER_FILE_NAME_PREFIX, newSupervisor, ESTIMATED_MAX_FILE_SIZE);
	}

	ObjectContainerSupervisor(String directoryName, String CONTAINER_FILE_NAME_PREFIX, boolean newSupervisor, long MAX_FILE_SIZE) throws ContainerException {
		if (CONTAINER_FILE_NAME_PREFIX.equals(SUPERVISOR_INFO_FILE_NAME)) {
			throw  new ContainerException("CONTAINER_FILE_NAME_PREFIX could not be " + SUPERVISOR_INFO_FILE_NAME);
		}
		this.CONTAINER_FILE_NAME_PREFIX = CONTAINER_FILE_NAME_PREFIX;
		this.MAX_FILE_SIZE = MAX_FILE_SIZE;
		this.directoryName = directoryName;
		this.CONTAINER_PATH_START = new StringBuilder().append(this.directoryName)
											.append(System.getProperty("file.separator"))
											.append(this.CONTAINER_FILE_NAME_PREFIX).toString();
		this.SUPERVISOR_INFO_FILE_PATH = new StringBuilder().append(this.directoryName)
												 .append(System.getProperty("file.separator"))
												 .append(SUPERVISOR_INFO_FILE_NAME).toString();
		try (FileReaderWriter rw = FileReaderWriter.openForReadingWriting(SUPERVISOR_INFO_FILE_PATH)) {
			if (newSupervisor) {
				File file = new File(SUPERVISOR_INFO_FILE_PATH);
				if (file.exists()) {
					file.delete();
				}
				rw.writeInt(0, 0);
			}
			nextContainerNumber = parseMaxContainerNumber(rw);
		} catch (IOException e) {
			throw  new ContainerException(e);
		}
	}

	private int parseMaxContainerNumber(FileReaderWriter in) throws IOException {
		return in.readInt(0);
	}

	public long getMaxObjectSize(int objectsCount) {
		return ((MAX_FILE_SIZE - TypeSizes.BYTES_IN_LONG) / objectsCount) - (ObjectContainer.getNeededSpace(new byte[0]) * objectsCount);
	}

	private String getContainerFileName(int number) {
		return new StringBuilder().append(CONTAINER_PATH_START)
					   .append(number).toString();
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

				String containerFileName = getContainerFileName(containerIndex);
				try (FileReaderWriter rw = FileReaderWriter.openForReadingWriting(containerFileName)) {
					new ObjectContainer(rw, containerFileName, containerIndex, false).removeBytes(rw, positions);
				}
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

				String nextContainerName = getContainerFileName(nextContainerNumber);
				new File(nextContainerName).createNewFile();
				try (FileReaderWriter rw = FileReaderWriter.openForReadingWriting(nextContainerName)) {
					ObjectContainer container =
							new ObjectContainer(rw, nextContainerName, nextContainerNumber, true);
					nextContainerNumber++;

					ObjectAddress[] subResult = container.writeBytes(rw, ID, bytes, startObject, curCount);
					System.arraycopy(subResult, 0, result, pointer, subResult.length);
					pointer += subResult.length;
				}
				startObject += curCount;
				curCount = 0;
			}
			return result;
		} catch (IOException e) {
			throw new ContainerException(e);
		}
	}

	public RecordData get(ObjectAddress address) throws ContainerException {
		return get(new ObjectAddress[] {address})[0];
	}

	public RecordData[] get(ObjectAddress[] addresses) throws ContainerException {
		try {
			RecordData[] result = new RecordData[addresses.length];
			Map<Integer, List<Integer>> addressesByContainer = new HashMap<>(addresses.length);
			for (int i = 0; i < addresses.length; i++) {
				ObjectAddress address = addresses[i];;
				if (address == null || address == ObjectAddress.EMPTY_ADDRESS) {
					continue;
				}
				int fileNumber = address.getFileNumber();
				if (!addressesByContainer.containsKey(fileNumber)) {
					addressesByContainer.put(fileNumber, new LinkedList<Integer>());
				}
				addressesByContainer.get(fileNumber).add(i);
			}
			for (Integer containerIndex : addressesByContainer.keySet()) {
				List<Integer> addressesIndecies = addressesByContainer.get(containerIndex);
				long[] positions = getPositionsFromAddressList(addresses, addressesIndecies);

				RecordData[] subResult;
				String containerFileName = getContainerFileName(containerIndex);
				try (FileReaderWriter in = FileReaderWriter.openForReading(containerFileName)) {
					subResult = new ObjectContainer(in, containerFileName, containerIndex, false).getData(in, positions);
				}
				for (int i = 0; i < addressesIndecies.size(); i++) {
					result[addressesIndecies.get(i)] = subResult[i];
				}
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
			ObjectAddress address = iterator.next();
			if (address == null || address == ObjectAddress.EMPTY_ADDRESS) {
				result[pointer++] = -1;
			} else {
				result[pointer++] = address.getFilePosition();
			}
		}
		return result;
	}

	private long[] getPositionsFromAddressList(ObjectAddress[] addresses, List<Integer> addresesIndecies) {
		long[] result = new long[addresesIndecies.size()];
		for (int i = 0; i < addresesIndecies.size(); i++) {
			result[i] = addresses[addresesIndecies.get(i)].getFilePosition();
		}
		return result;
	}

	private Map<Integer, List<ObjectAddress>> spreadByContainerNumber(ObjectAddress[] addresses) {
		Map<Integer, List<ObjectAddress>> result = new TreeMap<>();
		for (ObjectAddress address : addresses) {
			if (address == null || address == ObjectAddress.EMPTY_ADDRESS) {
				continue;
			}
			int fileNumber = address.getFileNumber();
			if (!result.containsKey(fileNumber)) {
				result.put(fileNumber, new LinkedList<ObjectAddress>());
			}
			result.get(fileNumber).add(address);
		}
		return result;
	}
}
