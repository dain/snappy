/*
 * Created: Mar 11, 2013
 */
package org.iq80.snappy;

import static org.iq80.snappy.SnappyFramed.COMPRESSED_DATA_FLAG;
import static org.iq80.snappy.SnappyFramed.HEADER_BYTES;
import static org.iq80.snappy.SnappyFramed.UNCOMPRESSED_DATA_FLAG;
import static org.iq80.snappy.SnappyInternalUtils.checkArgument;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Implements the <a
 * href="http://snappy.googlecode.com/svn/trunk/framing_format.txt"
 * >x-snappy-framed</a> as an {@link OutputStream}.
 * 
 * @author Brett Okken
 */
public final class SnappyFramedOutputStream extends AbstractSnappyOutputStream {
    /**
     * However, we place an additional restriction that the uncompressed data in
     * a chunk must be no longer than 65536 bytes. This allows consumers to
     * easily use small fixed-size buffers.
     */
    public static final int MAX_BLOCK_SIZE = 65536;

    public static final int DEFAULT_BLOCK_SIZE = MAX_BLOCK_SIZE;

    public static final double DEFAULT_MIN_COMPRESSION_RATIO = 0.85d;

    /**
     * 
     * @param out
     * @throws IOException
     */
    public SnappyFramedOutputStream(OutputStream out) throws IOException {
        this(out, DEFAULT_BLOCK_SIZE, DEFAULT_MIN_COMPRESSION_RATIO);
    }

    /**
     * @param out
     * @param blockSize
     * @param minCompressionRatio
     * @throws IOException
     */
    public SnappyFramedOutputStream(OutputStream out, int blockSize,
            double minCompressionRatio) throws IOException {
        super(out, blockSize, minCompressionRatio);
        checkArgument(blockSize > 0 && blockSize <= MAX_BLOCK_SIZE,
                "blockSize must be in (0, 65536]", blockSize);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void writeHeader(OutputStream out) throws IOException {
        out.write(HEADER_BYTES);
    }

    /**
     * Each chunk consists first a single byte of chunk identifier, then a
     * three-byte little-endian length of the chunk in bytes (from 0 to
     * 16777215, inclusive), and then the data if any. The four bytes of chunk
     * header is not counted in the data length.
     */
    @Override
    protected void writeBlock(final OutputStream out, byte[] data, int offset,
            int length, boolean compressed, int crc32c) throws IOException {
        out.write(compressed ? COMPRESSED_DATA_FLAG : UNCOMPRESSED_DATA_FLAG);

        // the length written out to the header is both the checksum and the
        // frame
        final int headerLength = length + 4;

        // write length
        out.write(headerLength);
        out.write(headerLength >>> 8);
        out.write(headerLength >>> 16);

        // write crc32c of user input data
        out.write(crc32c);
        out.write(crc32c >>> 8);
        out.write(crc32c >>> 16);
        out.write(crc32c >>> 24);

        // write data
        out.write(data, offset, length);
    }

}
