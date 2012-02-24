package org.fusesource.leveldbjni.internal;

public class ChunkHelper {
    public static class Chunk {
        public int size;
        public int[] keyIndices;
        public int[] valIndices;
        public byte[] keys;
        public byte[] values;

        private byte[] derefIndex(byte[] data, int[] indices, int index) {
            int limit = data.length;
            if (index < (size - 1)) {
                limit = indices[index + 1];
            }

            byte[] temp = new byte[limit - indices[index]];

            System.arraycopy(data, indices[index], temp, 0, limit - indices[index]);

            return temp;
        }

        public byte[] keyAt(int index) {
            return derefIndex(keys, keyIndices, index);
        }
            
        public byte[] valAt(int index) {
            return derefIndex(values, valIndices, index);
        }
    }

    public static final native Chunk nextChunk(long iter, long size);
}