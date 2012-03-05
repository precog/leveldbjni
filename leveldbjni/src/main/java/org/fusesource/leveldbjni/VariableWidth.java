package org.fusesource.leveldbjni;

import java.nio.ByteBuffer;

/**
 * This class represents variable-width keys/values in a 
 * chunked representation, as encoded by a prefix 32-bit
 * big-endian integer length.
 */
public class VariableWidth extends DataWidth {
    public int getWidth(ByteBuffer stream) {
        return stream.getInt();
    }
    
    @Override
    public boolean isRunLengthEncoded() {
        return true;
    }
}