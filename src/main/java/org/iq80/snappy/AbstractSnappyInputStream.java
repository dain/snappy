/*
 * Created: Mar 14, 2013
 */
package org.iq80.snappy;

import static java.lang.Math.min;
import static org.iq80.snappy.SnappyInternalUtils.checkNotNull;
import static org.iq80.snappy.SnappyInternalUtils.checkPositionIndexes;
import static org.iq80.snappy.SnappyInternalUtils.readBytes;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

/**
 * A common base class for frame based snappy input streams.
 * 
 * @author Brett Okken
 */
abstract class AbstractSnappyInputStream extends InputStream {
    private final InputStream in;
    private final byte[] frameHeader;
    private final boolean verifyChecksums;
    private final BufferRecycler recycler;

    /**
     * A single frame read from the underlying {@link InputStream}.
     */
    private byte[] input;

    /**
     * The decompressed data from {@link #input}.
     */
    private byte[] uncompressed;

    /**
     * Indicates if this instance has been closed.
     */
    private boolean closed;

    /**
     * Indicates if we have reached the EOF on {@link #in}.
     */
    private boolean eof;

    /**
     * The position in {@link buffer} to read to.
     */
    private int valid;

    /**
     * The next position to read from {@link #buffer}.
     */
    private int position;

    /**
     * Buffer is a reference to the real buffer of uncompressed data for the
     * current block: uncompressed if the block is compressed, or input if it is
     * not.
     */
    private byte[] buffer;

    /**
     * Creates a Snappy input stream to read data from the specified underlying
     * input stream.
     * 
     * @param in
     *            the underlying input stream
     * @param verifyChecksums
     *            if true, checksums in input stream will be verified
     */
    public AbstractSnappyInputStream(InputStream in, int maxBlockSize,
            int frameHeaderSize, boolean verifyChecksums) throws IOException {
        this.in = in;
        this.verifyChecksums = verifyChecksums;
        recycler = BufferRecycler.instance();
        allocateBuffersBasedOnSize(maxBlockSize + 5);
        this.frameHeader = new byte[frameHeaderSize];

        // stream must begin with stream header
        final byte[] expectedHeader = getExpectedHeader();
        final byte[] actualHeader = new byte[expectedHeader.length];

        final int read = readBytes(in, actualHeader, 0, actualHeader.length);
        if (read < expectedHeader.length) {
            throw new EOFException(
                    "encountered EOF while reading stream header");
        }
        if (!Arrays.equals(expectedHeader, actualHeader)) {
            throw new IOException("invalid stream header");
        }
    }

    /**
     * @param size
     */
    private void allocateBuffersBasedOnSize(int size) {
        input = recycler.allocInputBuffer(size);
        uncompressed = recycler.allocDecodeBuffer(size);
    }

    protected abstract byte[] getExpectedHeader();

    @Override
    public int read() throws IOException {
        if (closed) {
            return -1;
        }
        if (!ensureBuffer()) {
            return -1;
        }
        return buffer[position++] & 0xFF;
    }

    @Override
    public int read(byte[] output, int offset, int length) throws IOException {
        checkNotNull(output, "output is null");
        checkPositionIndexes(offset, offset + length, output.length);
        if (closed) {
            throw new IOException("Stream is closed");
        }

        if (length == 0) {
            return 0;
        }
        if (!ensureBuffer()) {
            return -1;
        }

        final int size = min(length, available());
        System.arraycopy(buffer, position, output, offset, size);
        position += size;
        return size;
    }

    @Override
    public int available() throws IOException {
        if (closed) {
            return 0;
        }
        return valid - position;
    }

    @Override
    public void close() throws IOException {
        try {
            in.close();
        } finally {
            if (!closed) {
                closed = true;
                recycler.releaseInputBuffer(input);
                recycler.releaseDecodeBuffer(uncompressed);
            }
        }
    }

    static enum FrameAction {
        RAW, SKIP, UNCOMPRESS;
    }

    public static final class FrameMetaData {
        final int length;
        final FrameAction frameAction;

        /**
         * @param frameAction
         * @param length
         */
        public FrameMetaData(FrameAction frameAction, int length) {
            super();
            this.frameAction = frameAction;
            this.length = length;
        }
    }

    public static final class FrameData {
        final int checkSum;
        final int offset;

        /**
         * @param checkSum
         * @param offset
         */
        public FrameData(int checkSum, int offset) {
            super();
            this.checkSum = checkSum;
            this.offset = offset;
        }
    }

    private boolean ensureBuffer() throws IOException {
        if (available() > 0) {
            return true;
        }
        if (eof) {
            return false;
        }

        if (!readBlockHeader()) {
            eof = true;
            return false;
        }

        // get action based on header
        final FrameMetaData frameMetaData = getFrameMetaData(frameHeader);

        if (FrameAction.SKIP == frameMetaData.frameAction) {
            SnappyInternalUtils.skip(in, frameMetaData.length);
            return ensureBuffer();
        }

        if (frameMetaData.length > input.length) {
            allocateBuffersBasedOnSize(frameMetaData.length);
        }

        final int actualRead = readBytes(in, input, 0, frameMetaData.length);
        if (actualRead != frameMetaData.length) {
            throw new EOFException("unexpectd EOF when reading frame");
        }

        final FrameData frameData = getFrameData(frameHeader, input, actualRead);

        if (FrameAction.UNCOMPRESS == frameMetaData.frameAction) {
            final int uncompressedLength = Snappy.getUncompressedLength(input,
                    frameData.offset);

            if (uncompressedLength > uncompressed.length) {
                uncompressed = recycler.allocDecodeBuffer(uncompressedLength);
            }

            this.valid = Snappy.uncompress(input, frameData.offset, actualRead
                    - frameData.offset, uncompressed, 0);
            this.buffer = uncompressed;
            this.position = 0;
        } else {
            // we need to start reading at the offset
            this.position = frameData.offset;
            this.buffer = input;
            // valid is until the end of the read data, regardless of offset
            // indicating where we start
            this.valid = actualRead;
        }

        if (verifyChecksums) {
            final int actualCrc32c = Crc32C.maskedCrc32c(buffer, position,
                    valid - position);
            if (frameData.checkSum != actualCrc32c) {
                throw new IOException("Corrupt input: invalid checksum");
            }
        }

        return true;
    }

    /**
     * Use the content of the frameHeader to describe what type of frame we have
     * and the action to take.
     */
    protected abstract FrameMetaData getFrameMetaData(byte[] frameHeader)
            throws IOException;

    /**
     * Take the frame header and the content of the frame to describe metadata
     * about the content.
     * 
     * @param frameHeader
     *            The frame header.
     * @param content
     *            The content of the of the frame. Content begins at index
     *            {@code 0}.
     * @param length
     *            The length of the content.
     * @return Metadata about the content of the frame.
     * @throws IOException
     */
    protected abstract FrameData getFrameData(byte[] frameHeader,
            byte[] content, int length) throws IOException;

    private boolean readBlockHeader() throws IOException {
        int read = readBytes(in, frameHeader, 0, frameHeader.length);

        if (read == -1) {
            return false;
        }

        if (read < frameHeader.length) {
            throw new EOFException("encountered EOF while reading block header");
        }

        return true;
    }
}
