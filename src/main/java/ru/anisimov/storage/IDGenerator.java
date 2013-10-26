package ru.anisimov.storage;

import ru.anisimov.storage.exceptions.IDGeneratorException;

/**
 * @author Ivan Anisimov (ivananisimov2010@gmail.com)
 */
interface IDGenerator {
	long generateID() throws IDGeneratorException;
	void addFreeID(long ID) throws IDGeneratorException;
}
