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

import static org.iq80.snappy.SnappyFramed.COMPRESSED_DATA_FLAG;
import static org.iq80.snappy.SnappyFramed.HEADER_BYTES;
import static org.iq80.snappy.SnappyFramed.STREAM_IDENTIFIER_FLAG;
import static org.iq80.snappy.SnappyFramed.UNCOMPRESSED_DATA_FLAG;
import static org.iq80.snappy.SnappyFramedOutputStream.MAX_BLOCK_SIZE;

/**
 * Implements the <a href="http://snappy.googlecode.com/svn/trunk/framing_format.txt" >x-snappy-framed</a> as an {@link InputStream}.
 */
public class SnappyFramedInputStream
        extends AbstractSnappyInputStream
{
    public SnappyFramedInputStream(InputStream in, boolean verifyChecksums)
            throws IOException
    {
        super(in, MAX_BLOCK_SIZE, 4, verifyChecksums, HEADER_BYTES);
    }

    @Override
    protected FrameMetaData getFrameMetaData(byte[] frameHeader)
            throws IOException
    {
        int length = (frameHeader[1] & 0xFF);
        length |= (frameHeader[2] & 0xFF) << 8;
        length |= (frameHeader[3] & 0xFF) << 16;

        int minLength;
        FrameAction frameAction;
        int flag = frameHeader[0] & 0xFF;
        switch (flag) {
            case COMPRESSED_DATA_FLAG:
                frameAction = FrameAction.UNCOMPRESS;
                minLength = 5;
                break;
            case UNCOMPRESSED_DATA_FLAG:
                frameAction = FrameAction.RAW;
                minLength = 5;
                break;
            case STREAM_IDENTIFIER_FLAG:
                if (length != 6) {
                    throw new IOException("stream identifier chunk with invalid length: " + length);
                }
                frameAction = FrameAction.SKIP;
                minLength = 6;
                break;
            default:
                // Reserved unskippable chunks (chunk types 0x02-0x7f)
                if (flag <= 0x7f) {
                    throw new IOException("unsupported unskippable chunk: " + Integer.toHexString(flag));
                }

                // all that is left is Reserved skippable chunks (chunk types 0x80-0xfe)
                frameAction = FrameAction.SKIP;
                minLength = 0;
        }

        if (length < minLength) {
            throw new IOException("invalid length: " + length + " for chunk flag: " + Integer.toHexString(flag));
        }

        return new FrameMetaData(frameAction, length);
    }

    @Override
    protected FrameData getFrameData(byte[] frameHeader, byte[] content, int length)
    {
        // crc is contained in the frame content
        int crc32c = (content[3] & 0xFF) << 24 |
                (content[2] & 0xFF) << 16 |
                (content[1] & 0xFF) << 8 |
                (content[0] & 0xFF);

        return new FrameData(crc32c, 4);
    }
}
