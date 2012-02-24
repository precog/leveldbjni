package org.fusesource.leveldbjni.internal;

import org.fusesource.leveldbjni.KeyValueChunk;

/**
 * This class serves as a wrapper for a JNI function to handle chunking
 * of data.
 *
 * TODO: remove once JNIEnv is available to hawtjni methods (hawtjni 1.6)
 */
public class ChunkHelper {
    public static final native KeyValueChunk nextChunk(long iter, long size);
}