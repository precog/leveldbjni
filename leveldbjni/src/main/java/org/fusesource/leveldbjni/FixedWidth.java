package org.fusesource.leveldbjni;

import java.nio.ByteBuffer;

/**
 * This class represents fixed-width keys/values in a 
 * chunked representation. Note that the library doesn't
 * enforce fixed-width data, so the user is responsible
 * for ensuring sane usage.
 */
public class FixedWidth extends DataWidth {
    private int width;

    public FixedWidth(int width) {
        this.width = width;
    }

    public int getWidth(ByteBuffer stream) {
        return width;
    }

    @Override
    public int getEncodingWidth() {
        return width;
    }
}