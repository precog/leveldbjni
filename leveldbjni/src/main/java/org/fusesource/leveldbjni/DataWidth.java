package org.fusesource.leveldbjni;

import java.nio.ByteBuffer;

/**
 * This interface allows us to determine the width of given keys/values in a chunked
 * representation based on the current byte stream.
 */
abstract public class DataWidth {
    // Make constructor private to prevent misuse
    DataWidth() {};
    
    /**
     * Compute the width of the field from the provided stream. After
     * this method is called the stream should be positioned at the 
     * start of the value, so implementers will likely want to use
     * mark/reset if the width is encoded in the content itself.
     */
    public abstract int getWidth(ByteBuffer stream);

    /**
     * Indicates whether to run-length encode the width of the value
     * using a 32-bit big-endian integer.
     */
    public boolean isRunLengthEncoded() {
        return false;
    }

    /**
     * Indicates the fixed width of the value. Zero indicates to use whatever the full
     * width of the value is as returned by leveldb.
     */
    public int getEncodingWidth() {
        return 0;
    }

    // Convenience (syntax) methods/properties
    public final static DataWidth VARIABLE = new VariableWidth();
    public final static DataWidth FIXED(int width) {
        return new FixedWidth(width);
    }
}