package org.fusesource.leveldbjni;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * This class represents a "chunk" of key/value pairs. Consolidating 
 * multiple pairs into a single object instance reduces the penalty
 * for crossing the JNI boundary when talking to the underlying leveldb
 * library.
 *
 * Although an Iterator interface is provided for convenience,
 * all members are public to allow for optimal performance in
 * use cases that can directly consume the byte stream.
 *
 * Data is either run-length-encoded or fixed-width key/value pairs. If run-length encoded,
 * lengths are 32 bit, big-endian Integers,
 * e.g. 32-bit length, key bytes, 32-bit length, value bytes, ...
 * or  fixed-width key bytes, 32-bit length, value bytes, etc
 *
 */
public class KeyValueChunk {
    // How many bytes of the buffer are actually used
    public int byteLength;

    // How many pairs are contained in this chunk
    public int pairLength;

    // Encodings
    public DataWidth keyWidth;
    public DataWidth valueWidth;

    public byte[] data;

    public KeyValueChunk(byte[] data, int byteLength, int pairLength, DataWidth keyWidth, DataWidth valueWidth) {
        this.data = data;
        this.byteLength = byteLength;
        this.pairLength = pairLength;
        this.keyWidth = keyWidth;
        this.valueWidth = valueWidth;
    }

    public class KeyValuePair {
        private byte[] key;
        private byte[] value;

        private KeyValuePair(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
        }

        public byte[] getKey() {
            return key;
        }

        public byte[] getValue() {
            return value;
        }
    }

    public int getSize() {
        return pairLength;
    }

    public Iterator<KeyValuePair> getIterator() {
        return new Iterator<KeyValuePair>() {
            private ByteBuffer backing = ByteBuffer.wrap(data, 0, byteLength);

            public boolean hasNext() {
                return backing.hasRemaining();
            }

            public KeyValuePair next() {
                if (! backing.hasRemaining()) {
                    throw new NoSuchElementException("Chunk limit reached");
                }

                int keyLen = keyWidth.getWidth(backing);
                byte[] key = new byte[keyLen];
                System.arraycopy(data, backing.position(), key, 0, keyLen);
                backing.position(backing.position() + keyLen);
                
                int valueLen = valueWidth.getWidth(backing);
                byte[] value = new byte[valueLen];
                System.arraycopy(data, backing.position(), value, 0, valueLen);
                backing.position(backing.position() + valueLen);

                return new KeyValuePair(key, value);
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
