/*
 * Created: Mar 14, 2013
 */
package org.iq80.snappy;

/**
 * Constants for implementing x-snappy-framed.
 * 
 * @author Brett Okken
 */
final class SnappyFramed {
    public static final int COMPRESSED_DATA_FLAG = 0x00;

    public static final int UNCOMPRESSED_DATA_FLAG = 0x01;

    public static final int STREAM_IDENTIFIER_FLAG = 0xff;

    /**
     * The header consists of the stream identifier flag, 3 bytes indicating a
     * length of 6, and "sNaPpY" in ASCII.
     */
    public static final byte[] HEADER_BYTES = new byte[] {
            (byte) STREAM_IDENTIFIER_FLAG, 0x06, 0x00, 0x00, 0x73, 0x4e, 0x61,
            0x50, 0x70, 0x59 };
}
