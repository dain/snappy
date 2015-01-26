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
import java.io.InputStream;
import java.util.Arrays;

import static org.iq80.snappy.SnappyFramed.HEADER_BYTES;
import static org.iq80.snappy.SnappyInternalUtils.checkArgument;
import static org.iq80.snappy.SnappyInternalUtils.checkNotNull;
import static org.iq80.snappy.SnappyOutputStream.STREAM_HEADER;

public final class Snappy
{

    private static final int MAX_HEADER_LENGTH = Math.max(STREAM_HEADER.length, HEADER_BYTES.length);

    private Snappy()
    {
    }

    /**
     * Uses the stream marker bytes to determine if the {@link SnappyFramedInputStream} or
     * {@link SnappyInputStream} should be used to decompress the content of <i>source</i>.
     *
     * @param source The compressed content to decompress. Must {@link InputStream#markSupported()
     * support} {@link InputStream#mark(int).}
     * @param verifyChecksums Indicates if the crc32-c checksums should be calculated and verified.
     * @return An appropriate {@link InputStream} implementation to decompress the content.
     * @throws IllegalArgumentException If <i>source</i> does not {@link InputStream#markSupported()
     * support} mark/reset or does not contain the appropriate marker bytes for either implementation.
     */
    @SuppressWarnings("deprecation")
    public static InputStream determineSnappyInputStream(InputStream source, boolean verifyChecksums)
            throws IOException
    {
        checkNotNull(source, "source is null");
        checkArgument(source.markSupported(), "source does not support mark/reset");

        // read the header and then reset to start of stream
        source.mark(MAX_HEADER_LENGTH);
        byte[] buffer = new byte[MAX_HEADER_LENGTH];
        int read = SnappyInternalUtils.readBytes(source, buffer, 0, MAX_HEADER_LENGTH);
        source.reset();

        if (read != STREAM_HEADER.length || read != HEADER_BYTES.length) {
            throw new IllegalArgumentException("invalid header");
        }

        if (buffer[0] == HEADER_BYTES[0]) {
            checkArgument(Arrays.equals(Arrays.copyOf(buffer, HEADER_BYTES.length), HEADER_BYTES), "invalid header");
            return new SnappyFramedInputStream(source, verifyChecksums);
        }
        else {
            checkArgument(Arrays.equals(Arrays.copyOf(buffer, STREAM_HEADER.length), STREAM_HEADER), "invalid header");
            return new SnappyInputStream(source, verifyChecksums);
        }
    }

    public static int getUncompressedLength(byte[] compressed, int compressedOffset)
            throws CorruptionException
    {
        return SnappyDecompressor.getUncompressedLength(compressed, compressedOffset);
    }

    public static byte[] uncompress(byte[] compressed, int compressedOffset, int compressedSize)
            throws CorruptionException
    {
        return SnappyDecompressor.uncompress(compressed, compressedOffset, compressedSize);
    }

    public static int uncompress(byte[] compressed, int compressedOffset, int compressedSize, byte[] uncompressed, int uncompressedOffset)
            throws CorruptionException
    {
        return SnappyDecompressor.uncompress(compressed, compressedOffset, compressedSize, uncompressed, uncompressedOffset);
    }

    public static int maxCompressedLength(int sourceLength)
    {
        return SnappyCompressor.maxCompressedLength(sourceLength);
    }

    public static int compress(
            byte[] uncompressed,
            int uncompressedOffset,
            int uncompressedLength,
            byte[] compressed,
            int compressedOffset)
    {
        return SnappyCompressor.compress(uncompressed,
                uncompressedOffset,
                uncompressedLength,
                compressed,
                compressedOffset);
    }


    public static byte[] compress(byte[] data)
    {
        byte[] compressedOut = new byte[maxCompressedLength(data.length)];
        int compressedSize = compress(data, 0, data.length, compressedOut, 0);
        byte[] trimmedBuffer = Arrays.copyOf(compressedOut, compressedSize);
        return trimmedBuffer;
    }

    static final int LITERAL = 0;
    static final int COPY_1_BYTE_OFFSET = 1;  // 3 bit length + 3 bits of offset in opcode
    static final int COPY_2_BYTE_OFFSET = 2;
    static final int COPY_4_BYTE_OFFSET = 3;
}
