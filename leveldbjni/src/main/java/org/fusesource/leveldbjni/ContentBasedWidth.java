package org.fusesource.leveldbjni;

import java.nio.ByteBuffer;

/**
 * This class represents variable-width keys/values in a 
 * chunked representation, as encoded by the content itself.
 * As such, this is a base class only. The end user must provide
 * a concrete implementation to compute length from the stream.
 */
public abstract class ContentBasedWidth extends DataWidth {
}