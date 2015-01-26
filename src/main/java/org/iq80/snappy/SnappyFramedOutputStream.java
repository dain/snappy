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

import static org.iq80.snappy.SnappyFramed.COMPRESSED_DATA_FLAG;
import static org.iq80.snappy.SnappyFramed.HEADER_BYTES;
import static org.iq80.snappy.SnappyFramed.UNCOMPRESSED_DATA_FLAG;
import static org.iq80.snappy.SnappyInternalUtils.checkArgument;

/**
 * Implements the <a href="http://snappy.googlecode.com/svn/trunk/framing_format.txt" >x-snappy-framed</a> as an {@link OutputStream}.
 */
public final class SnappyFramedOutputStream
        extends AbstractSnappyOutputStream
{
    /**
     * We place an additional restriction that the uncompressed data in
     * a chunk must be no longer than 65536 bytes. This allows consumers to
     * easily use small fixed-size buffers.
     */
    public static final int MAX_BLOCK_SIZE = 65536;

    public static final int DEFAULT_BLOCK_SIZE = MAX_BLOCK_SIZE;

    public static final double DEFAULT_MIN_COMPRESSION_RATIO = 0.85d;

    public SnappyFramedOutputStream(OutputStream out)
            throws IOException
    {
        this(out, DEFAULT_BLOCK_SIZE, DEFAULT_MIN_COMPRESSION_RATIO);
    }

    public SnappyFramedOutputStream(OutputStream out, int blockSize,
            double minCompressionRatio)
            throws IOException
    {
        super(out, blockSize, minCompressionRatio);
        checkArgument(blockSize > 0 && blockSize <= MAX_BLOCK_SIZE, "blockSize must be in (0, 65536]", blockSize);
    }

    @Override
    protected void writeHeader(OutputStream out)
            throws IOException
    {
        out.write(HEADER_BYTES);
    }

    /**
     * Each chunk consists first a single byte of chunk identifier, then a
     * three-byte little-endian length of the chunk in bytes (from 0 to
     * 16777215, inclusive), and then the data if any. The four bytes of chunk
     * header is not counted in the data length.
     */
    @Override
    protected void writeBlock(OutputStream out, byte[] data, int offset, int length, boolean compressed, int crc32c)
            throws IOException
    {
        out.write(compressed ? COMPRESSED_DATA_FLAG : UNCOMPRESSED_DATA_FLAG);

        // the length written out to the header is both the checksum and the
        // frame
        int headerLength = length + 4;

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
