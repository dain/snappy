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

import static java.lang.String.format;
import static org.iq80.snappy.SnappyOutputStream.MAX_BLOCK_SIZE;
import static org.iq80.snappy.SnappyOutputStream.STREAM_HEADER;

/**
 * This class implements an input stream for reading Snappy compressed data
 * of the format produced by {@link SnappyOutputStream}.
 * <p>
 * <b>NOTE:</b>This implementation cannot read compressed data produced
 * by {@link SnappyFramedOutputStream}.
 * </p>
 *
 * @deprecated Prefer the use of {@link SnappyFramedInputStream} which implements
 * the standard {@code x-snappy-framed} specification.
 */
@Deprecated
public class SnappyInputStream
        extends AbstractSnappyInputStream
{
    private static final int HEADER_LENGTH = 7;

    /**
     * Creates a Snappy input stream to read data from the specified underlying input stream.
     *
     * @param in the underlying input stream
     */
    public SnappyInputStream(InputStream in)
            throws IOException
    {
        this(in, true);
    }

    /**
     * Creates a Snappy input stream to read data from the specified underlying input stream.
     *
     * @param in the underlying input stream
     * @param verifyChecksums if true, checksums in input stream will be verified
     */
    public SnappyInputStream(InputStream in, boolean verifyChecksums)
            throws IOException
    {
        super(in, MAX_BLOCK_SIZE, HEADER_LENGTH, verifyChecksums, STREAM_HEADER);
    }

    @Override
    protected FrameMetaData getFrameMetaData(byte[] frameHeader)
            throws IOException
    {
        int x = frameHeader[0] & 0xFF;

        int a = frameHeader[1] & 0xFF;
        int b = frameHeader[2] & 0xFF;
        int length = (a << 8) | b;

        FrameAction action;
        switch (x) {
            case 0x00:
                action = FrameAction.RAW;
                break;
            case 0x01:
                action = FrameAction.UNCOMPRESS;
                break;
            case 's':
                if (!Arrays.equals(STREAM_HEADER, frameHeader)) {
                    throw new IOException(format("invalid compressed flag in header: 0x%02x", x));
                }
                action = FrameAction.SKIP;
                length = 0;
                break;
            default:
                throw new IOException(format("invalid compressed flag in header: 0x%02x", x));
        }

        if (((length <= 0) || (length > MAX_BLOCK_SIZE)) && action != FrameAction.SKIP) {
            throw new IOException("invalid block size in header: " + length);
        }

        return new FrameMetaData(action, length);
    }

    @Override
    protected FrameData getFrameData(byte[] frameHeader, byte[] content, int length)
    {
        // crc is contained in the frame header
        int crc32c = (frameHeader[3] & 0xFF) << 24 |
                (frameHeader[4] & 0xFF) << 16 |
                (frameHeader[5] & 0xFF) << 8 |
                (frameHeader[6] & 0xFF);

        return new FrameData(crc32c, 0);
    }
}
