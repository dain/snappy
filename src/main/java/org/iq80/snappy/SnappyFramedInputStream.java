/*
 * Created: Mar 15, 2013
 */
package org.iq80.snappy;

import static org.iq80.snappy.SnappyFramed.COMPRESSED_DATA_FLAG;
import static org.iq80.snappy.SnappyFramed.HEADER_BYTES;
import static org.iq80.snappy.SnappyFramed.STREAM_IDENTIFIER_FLAG;
import static org.iq80.snappy.SnappyFramed.UNCOMPRESSED_DATA_FLAG;
import static org.iq80.snappy.SnappyFramedOutputStream.MAX_BLOCK_SIZE;

import java.io.IOException;
import java.io.InputStream;

/**
 * Implements the <a
 * href="http://snappy.googlecode.com/svn/trunk/framing_format.txt"
 * >x-snappy-framed</a> as an {@link InputStream}.
 * 
 * @author Brett Okken
 */
public class SnappyFramedInputStream extends AbstractSnappyInputStream {

    /**
     * 
     * @param in
     * @param verifyChecksums
     * @throws IOException
     */
    public SnappyFramedInputStream(InputStream in, boolean verifyChecksums)
            throws IOException {
        super(in, MAX_BLOCK_SIZE, 4, verifyChecksums);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected byte[] getExpectedHeader() {
        return HEADER_BYTES;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected FrameMetaData getFrameMetaData(byte[] frameHeader)
            throws IOException {
        int length = (frameHeader[1] & 0xFF);
        length |= (frameHeader[2] & 0xFF) << 8;
        length |= (frameHeader[3] & 0xFF) << 16;

        int minLength = 0;
        final FrameAction frameAction;
        final int flag = frameHeader[0] & 0xFF;
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
                    throw new IOException(
                            "stream identifier chunk with invalid length: "
                                    + length);
                }
                frameAction = FrameAction.SKIP;
                minLength = 6;
                break;
            default:
                // Reserved unskippable chunks (chunk types 0x02-0x7f)
                if (flag <= 0x7f) {
                    throw new IOException("unsupported unskippable chunk: "
                            + Integer.toHexString(flag));
                }

                // all that is left is Reserved skippable chunks (chunk types
                // 0x80-0xfe)
                frameAction = FrameAction.SKIP;
                minLength = 0;
        }

        if (length < minLength) {
            throw new IOException("invalid length: " + length
                    + " for chunk flag: " + Integer.toHexString(flag));
        }

        return new FrameMetaData(frameAction, length);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected FrameData getFrameData(byte[] frameHeader, byte[] content,
            int length) throws IOException {
        return new FrameData(getCrc32c(content), 4);
    }

    private int getCrc32c(byte[] content) {
        return ((content[3] & 0xFF) << 24) | ((content[2] & 0xFF) << 16)
                | ((content[1] & 0xFF) << 8) | (content[0] & 0xFF);
    }
}
