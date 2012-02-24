package org.fusesource.leveldbjni;

/**
 * This class represents a "chunk" of key/value pairs. Consolidating 
 * multiple pairs into a single object instance reduces the penalty
 * for crossing the JNI boundary when talking to the underlying leveldb
 * library.
 */
public class KeyValueChunk {
    private int size;
    private int[] keyIndices;
    private int[] valIndices;
    private byte[] keys;
    private byte[] values;

    private byte[] derefIndex(byte[] data, int[] indices, int index) {
        int limit = data.length;
        if (index < (size - 1)) {
            limit = indices[index + 1];
        }

        byte[] temp = new byte[limit - indices[index]];

        System.arraycopy(data, indices[index], temp, 0, limit - indices[index]);

        return temp;
    }

    public int getSize() {
        return size;
    }

    /** 
     * Return the key byte array at the given index.
     */
    public byte[] keyAt(int index) {
        return derefIndex(keys, keyIndices, index);
    }
       
    /**
     * Return the value byte array at the given index.
     */
    public byte[] valAt(int index) {
        return derefIndex(values, valIndices, index);
    }
}
