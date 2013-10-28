package ru.anisimov.storage.localStorage;

import ru.anisimov.storage.RebuildInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Ivan Anisimov (ivananisimov2010@gmail.com)
 */
public class DirectoryStorageRebuildInfo implements RebuildInfo {
	private List<String> lostContainers;

	private DirectoryStorageRebuildInfo() {
		lostContainers = new ArrayList<>();
	}

	@Override
	public String getMessage() {
		return "Lost containers: " + Arrays.toString(lostContainers.toArray());
	}

	public static class Builder {
		private DirectoryStorageRebuildInfo info;

		public Builder() {
			info = new DirectoryStorageRebuildInfo();
		}

		public Builder addLostContainer(String fileName) {
			info.lostContainers.add(fileName);
			return this;
		}

		public DirectoryStorageRebuildInfo build() {
			return info;
		}
	}
}
