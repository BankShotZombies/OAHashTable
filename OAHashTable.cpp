/**
 * @file OAHashTable.cpp
 * @author Adam Lonstein (adam.lonstein@digipen.com)
 * @brief This implements a hash table. The hash table can use both linear probing and double hashing
 *        collision resolutions. The hash table can use both MARK and PACK deletion policies. The hashing
 *        function(s) must be client-provided.
 * @date 04-12-2024
 */

#include <cmath>

/**
 * @brief Initializes the config, stats, and table
 */
template<typename T>
OAHashTable<T>::OAHashTable(const OAHTConfig& Config) : mTable(new OAHTSlot[Config.InitialTableSize_]), mConfig(Config), mStats()
{
    mStats.TableSize_ = Config.InitialTableSize_;

    mStats.PrimaryHashFunc_ = mConfig.PrimaryHashFunc_;
    mStats.SecondaryHashFunc_ = mConfig.SecondaryHashFunc_;

    // Set all slots in table to unoccupied
    for(unsigned int i = 0; i < mConfig.InitialTableSize_; ++i)
    {
        mTable[i].State = OAHTSlot::OAHTSlot_State::UNOCCUPIED;
    }
}

/**
 * @brief Deletes the hash table
 */
template<typename T>
OAHashTable<T>::~OAHashTable()
{
    clear();

    delete [] mTable;
}

/**
 * @brief Grows the table if the load factor is greater than max load factor. Then inserts into the table.
 * 
 * @param Key - key to insert
 * @param Data - data to insert
 */
template<typename T>
void OAHashTable<T>::insert(const char *Key, const T& Data)
{
    double loadFactor = static_cast<double>(mStats.Count_ + 1) / static_cast<double>(mStats.TableSize_);

    // Grow the table if needed
    if(loadFactor > mConfig.MaxLoadFactor_)
    {
        GrowTable();
    }

    // Insert the key/data into the table
    InsertInTable(mTable, mStats.TableSize_, Key, Data);
    mStats.Count_++;
}

/**
 * @brief Removes a slot from the table. Either uses MARK or PACK policy.
 * 
 * @param Key - key to remove
 */
template<typename T>
void OAHashTable<T>::remove(const char *Key)
{
    OAHTSlot* slot;

    // Get the index of this key in the table
    int index = IndexOf(Key, slot);

    // Throw an exception if the index doesn't exist
    if(index == -1)
    {
        throw OAHashTableException(OAHashTableException::E_ITEM_NOT_FOUND, "Key not in table.");
    }
     
    // An element is getting removed
    mStats.Count_--;

    if(mConfig.DeletionPolicy_ == OAHTDeletionPolicy::PACK)
    {
        // Use the client-provided free policy on the element
        if(mConfig.FreeProc_)
            mConfig.FreeProc_(slot->Data);

        // Set the slot to unoccupied
        slot->State = OAHTSlot::OAHTSlot_State::UNOCCUPIED;

        int originalIndex = index;
        index++; // Go to the next index

        // Wrap around array if necessary
        if(static_cast<unsigned>(index) > mStats.TableSize_ - 1)
        {
            index = 0;
        }

        // For every remaining element in the cluster
        while(mTable[index].State == OAHTSlot::OAHTSlot_State::OCCUPIED && index != originalIndex)
        {
            // Set the element to unoccupied
            mTable[index].State = OAHTSlot::OAHTSlot_State::UNOCCUPIED;

            // Re-insert the element into the table
            InsertInTable(mTable, mStats.TableSize_, mTable[index].Key, mTable[index].Data);

            index++;
            
            // Wrap around array if necessary
            if(static_cast<unsigned>(index) > mStats.TableSize_ - 1)
            {
                index = 0;
            }
        }
    }
    else if(mConfig.DeletionPolicy_ == OAHTDeletionPolicy::MARK)
    {
        // Simple mark the element as deleted
        mTable[index].State = OAHTSlot::OAHTSlot_State::DELETED;
    }
}

/**
 * @brief Finds an element in the table by key
 * 
 * @param Key - the key to search for 
 * @return const T& - returns the data associated with the key
 */
template<typename T>
const T& OAHashTable<T>::find(const char *Key) const
{
    OAHTSlot* slot;

    // Get the index of the key (throw an exception if it doesn't exist)
    if(IndexOf(Key, slot) == -1)
        throw OAHashTableException(OAHashTableException::E_ITEM_NOT_FOUND, "Item not found in table.");

    return slot->Data; // Return the found slots data
}

/**
 * @brief Clears and cleans up the hash table
 */
template<typename T>
void OAHashTable<T>::clear()
{
    for (unsigned int i = 0; i < mStats.TableSize_; ++i)
    {
        if (mTable[i].State != OAHTSlot::OAHTSlot_State::UNOCCUPIED)
        {
            // Decrease count of the hash table (as long as it hasn't already through MARK policy)
            if(mTable[i].State != OAHTSlot::OAHTSlot_State::DELETED)
                --mStats.Count_;

            // Use the free policy to free the data
            if (mConfig.FreeProc_)
                mConfig.FreeProc_(mTable[i].Data);

            // The slot is now unoccupied
            mTable[i].State = OAHTSlot::OAHTSlot_State::UNOCCUPIED;
        }
    }
}

/**
 * @brief Returns the stats of the hash table.
 */
template<typename T>
OAHTStats OAHashTable<T>::GetStats() const
{
    return mStats;
}

/**
 * @brief Returns a pointer to the hash table.
 */
template<typename T>
const typename OAHashTable<T>::OAHTSlot* OAHashTable<T>::GetTable() const
{
    return mTable;
}

/**
 * @brief Inserts into a given table. This function assumes there will be room in the table.
 * 
 * @param table - the table to insert pair in
 * @param Key - the key to insert
 * @param Data - the data to insert
 */
template<typename T>
void OAHashTable<T>::InsertInTable(OAHTSlot* table, unsigned tableSize, const char* Key, const T& Data)
{
    unsigned index = mConfig.PrimaryHashFunc_(Key, tableSize);

    unsigned stride = 1;

    // If we are doing double hashing, get the stride/increment
    if (mConfig.SecondaryHashFunc_)
        stride = mConfig.SecondaryHashFunc_(Key, tableSize - 1) + 1;

    // Search for an open spot in the array
    while (table[index].State == OAHTSlot::OAHTSlot_State::OCCUPIED)
    {
        // Throw an exception if there's a duplicate
        if (strcmp(table[index].Key, Key) == 0)
        {
            throw OAHashTableException(OAHashTableException::E_DUPLICATE, "Found duplicate item.");
        }

        index += stride; // Go to the next index by stride

        // Wrap around the array if needed
        if (index > tableSize - 1)
        {
            index -= tableSize;
        }

        mStats.Probes_++;
    }

    // If the slot that was inserted into was a deleted slot, check for duplicates
    if (table[index].State == OAHTSlot::OAHTSlot_State::DELETED && mConfig.DeletionPolicy_ == OAHTDeletionPolicy::MARK)
    {
        CheckForMarkInsertionDuplicate(index, stride, table, tableSize, Key);
    }

    ++mStats.Probes_;

    // Insert the data into the slot
    strcpy(table[index].Key, Key);
    table[index].Data = Data;

    // The slot is now occupied
    table[index].State = OAHTSlot::OAHTSlot_State::OCCUPIED;

}

/**
 * @brief Grows the table (should only be called when load factor is past max load factor)
 */
template<typename T>
void OAHashTable<T>::GrowTable()
{
    // Calculate the new table size
    double factor = std::ceil(mStats.TableSize_ * mConfig.GrowthFactor_);
    unsigned newTableSize = GetClosestPrime(static_cast<unsigned>(factor));

    // Allocate the new table
    OAHTSlot* newTable = new OAHTSlot[newTableSize];

    // Set all slots in new table to unoccupied
    for(unsigned int i = 0; i < newTableSize; ++i)
    {
        newTable[i].State = OAHTSlot::OAHTSlot_State::UNOCCUPIED;
    }

    // Insert every slot in old table into new table
    for(unsigned int i = 0; i < mStats.TableSize_; ++i)
    {
        if(mTable[i].State == OAHTSlot::OAHTSlot_State::OCCUPIED)
        {
            InsertInTable(newTable, newTableSize, mTable[i].Key, mTable[i].Data);
        }
    }

    // Delete the old table
    delete [] mTable;

    // Set table to the new table
    mTable = newTable;

    mStats.TableSize_ = newTableSize;
    mStats.Expansions_++;
}

/**
 * @brief Finds the index of a key in the hash table
 * 
 * @param Key - key to find
 * @param Slot - sets this to the found slot
 * @return int - returns the index of the slot, -1 if not found
 */
template<typename T>
int OAHashTable<T>::IndexOf(const char *Key, OAHTSlot* &Slot) const
{
    // Get the index with the primary hash function
    unsigned index = mConfig.PrimaryHashFunc_(Key, mStats.TableSize_);

    unsigned stride = 1;

    // If we are doing double hashing, get the stride/increment
    if (mConfig.SecondaryHashFunc_)
        stride = mConfig.SecondaryHashFunc_(Key, mStats.TableSize_ - 1) + 1;

    unsigned originalIndex = index;

    if(mTable[index].State == OAHTSlot::OAHTSlot_State::UNOCCUPIED)
        ++mStats.Probes_;

    // Walk the table with stride until an unoccupied slot is found
    while(mTable[index].State == OAHTSlot::OAHTSlot_State::OCCUPIED || mTable[index].State == OAHTSlot::OAHTSlot_State::DELETED)
    {
        ++mStats.Probes_;

        // If this is the slot, return
        if(strcmp(mTable[index].Key, Key) == 0)
        {
            Slot = &mTable[index];

            return index;
        }

        index += stride;

        // Wrap around the array if needed
        if(static_cast<unsigned>(index) > mStats.TableSize_ - 1)
        {
            index -= mStats.TableSize_;
        }

        if(mTable[index].State == OAHTSlot::OAHTSlot_State::UNOCCUPIED)
            ++mStats.Probes_;

        // Stop if it has come back to the original index
        if(index == originalIndex)
        {
            break;
        }
    }

    // If an unoccupied slot was found, the key is not in the array
    return -1;
}

/**
 * @brief Prints the array, debug purposes
 */
template<typename T>
void OAHashTable<T>::PrintTable(OAHTSlot* table, unsigned tableSize) const
{
    for(unsigned i = 0; i < tableSize; ++i)
    {
        if(table[i].State == OAHTSlot::OAHTSlot_State::UNOCCUPIED)
            std::cout << "Index " << i << ": " << "unoccupied";
        else
            std::cout << "Index " << i << ": " << table[i].Key;

        std::cout << std::endl;
    }

    std::cout << std::endl;
}

/**
 * @brief Checks for a duplicate item after a slot marked as deleted was inserted into.
 * 
 * @param index - index of inserted slot
 * @param stride - stride
 * @param table - the table to check
 * @param tableSize - the table size
 * @param Key - the key that was inserted
 */
template<typename T>
void OAHashTable<T>::CheckForMarkInsertionDuplicate(unsigned index, unsigned stride, OAHTSlot* table, unsigned tableSize, const char* Key)
{
    unsigned duplicateCheckIndex = index + stride; // Go to the next index with stride

    // Walk through the table with stride until an unoccupied slot is found
    while (table[duplicateCheckIndex].State == OAHTSlot::OAHTSlot_State::DELETED || table[duplicateCheckIndex].State == OAHTSlot::OAHTSlot_State::OCCUPIED)
    {
        ++mStats.Probes_;

        // If any duplicates are found, throw an exception
        if (strcmp(table[duplicateCheckIndex].Key, Key) == 0)
        {
            throw OAHashTableException(OAHashTableException::E_DUPLICATE, "Found duplicate item.");
        }

        // Go to the next index with stride
        duplicateCheckIndex += stride;

        // Wrap around the array if needed
        if (duplicateCheckIndex > tableSize - 1)
        {
            duplicateCheckIndex -= tableSize;
        }
    }

    ++mStats.Probes_;
}
