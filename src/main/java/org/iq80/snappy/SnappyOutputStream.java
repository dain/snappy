/*
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.iq80.snappy;

import java.io.IOException;
import java.io.OutputStream;

/**
 * This class implements an output stream for writing Snappy compressed data.
 * The output format is the stream header "snappy\0" followed by one or more
 * compressed blocks of data, each of which is preceded by a seven byte header.
 * <p/>
 * The first byte of the header is a flag indicating if the block is compressed
 * or not. A value of 0x00 means uncompressed, and 0x01 means compressed.
 * <p/>
 * The second and third bytes are the size of the block in the stream as a big
 * endian number. This value is never zero as empty blocks are never written.
 * The maximum allowed length is 32k (1 << 15).
 * <p/>
 * The remaining four byes are crc32c checksum of the user input data masked
 * with the following function: {@code ((crc >>> 15) | (crc << 17)) + 0xa282ead8 }
 * <p/>
 * An uncompressed block is simply copied from the input, thus guaranteeing
 * that the output is never larger than the input (not including the header).
 * <p>
 * <b>NOTE:</b>This data produced by this class is not compatible with the
 * {@code x-snappy-framed} specification. It can only be read by
 * {@link SnappyInputStream}.
 * </p>
 *
 * @deprecated Use {@link SnappyFramedOutputStream} which implements
 * the standard {@code x-snappy-framed} specification.
 */
@Deprecated
public class SnappyOutputStream
        extends AbstractSnappyOutputStream
{
    static final byte[] STREAM_HEADER = new byte[] {'s', 'n', 'a', 'p', 'p', 'y', 0};

    // the header format requires the max block size to fit in 15 bits -- do not change!
    static final int MAX_BLOCK_SIZE = 1 << 15;

    /**
     * Write out the uncompressed content if the compression ratio (compressed length / raw length) exceeds this value.
     */
    public static final double MIN_COMPRESSION_RATIO = 7.0 / 8.0;

    private final boolean calculateChecksum;

    /**
     * Creates a Snappy output stream to write data to the specified underlying output stream.
     *
     * @param out the underlying output stream
     */
    public SnappyOutputStream(OutputStream out)
            throws IOException
    {
        this(out, true);
    }

    private SnappyOutputStream(OutputStream out, boolean calculateChecksum)
            throws IOException
    {
        super(out, MAX_BLOCK_SIZE, MIN_COMPRESSION_RATIO);
        this.calculateChecksum = calculateChecksum;
    }

    /**
     * Creates a Snappy output stream with block checksums disabled.  This is only useful for
     * apples-to-apples benchmarks with other compressors that do not perform block checksums.
     *
     * @param out the underlying output stream
     */
    public static SnappyOutputStream newChecksumFreeBenchmarkOutputStream(OutputStream out)
            throws IOException
    {
        return new SnappyOutputStream(out, false);
    }

    @Override
    protected void writeHeader(OutputStream out)
            throws IOException
    {
        out.write(STREAM_HEADER);
    }

    @Override
    protected int calculateCRC32C(byte[] data, int offset, int length)
    {
        return calculateChecksum ? super.calculateCRC32C(data, offset, length) : 0;
    }

    @Override
    protected void writeBlock(OutputStream out, byte[] data, int offset, int length, boolean compressed, int crc32c)
            throws IOException
    {
        // write compressed flag
        out.write(compressed ? 0x01 : 0x00);

        // write length
        out.write(length >>> 8);
        out.write(length);

        // write crc32c of user input data
        out.write(crc32c >>> 24);
        out.write(crc32c >>> 16);
        out.write(crc32c >>> 8);
        out.write(crc32c);

        // write data
        out.write(data, offset, length);
    }
}
