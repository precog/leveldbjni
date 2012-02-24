package org.fusesource.leveldbjni.internal;

public class ChunkHelper {
    public static final native byte[][][] nextChunk(long iter, long size);
}