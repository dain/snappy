/*
 * Created: Mar 14, 2013
 */
package org.iq80.snappy;

import static org.iq80.snappy.Crc32C.maskedCrc32c;
import static org.iq80.snappy.SnappyInternalUtils.checkArgument;
import static org.iq80.snappy.SnappyInternalUtils.checkNotNull;
import static org.iq80.snappy.SnappyInternalUtils.checkPositionIndexes;

import java.io.IOException;
import java.io.OutputStream;

/**
 * This is a base class supporting both the {@link SnappyOutputStream} and
 * {@link SnappyFramedOutputStream}.
 * 
 * <p>
 * Delegates writing the header bytes and individual frames to the specific
 * implementations. Implementations may also override the crc32 checksum
 * calculation.
 * </p>
 * 
 * @author Brett Okken
 * @since 0.4
 */
abstract class AbstractSnappyOutputStream extends OutputStream {
    private final BufferRecycler recycler;
    private final int blockSize;
    private final byte[] buffer;
    private final byte[] outputBuffer;
    private final double minCompressionRatio;

    private final OutputStream out;

    private int position;
    private boolean closed;

    /**
     * 
     * @param out
     *            The underlying {@link OutputStream} to write to. Must not be
     *            {@code null}.
     * @param blockSize
     *            The block size (of raw data) to compress before writing frames
     *            to <i>out</i>.
     * @param minCompressionRatio
     *            Defines the minimum compression ratio (
     *            {@code compressedLength / rawLength}) that must be achieved to
     *            write the compressed data. This must be in (0, 1.0].
     * @throws IOException
     */
    public AbstractSnappyOutputStream(OutputStream out, int blockSize,
            double minCompressionRatio) throws IOException {
        this.out = checkNotNull(out, "out is null");
        checkArgument(minCompressionRatio > 0 && minCompressionRatio <= 1.0,
                "minCompressionRatio %1s must be between (0,1.0].",
                minCompressionRatio);
        this.minCompressionRatio = minCompressionRatio;
        recycler = BufferRecycler.instance();
        this.blockSize = blockSize;
        buffer = recycler.allocOutputBuffer(blockSize);
        outputBuffer = recycler.allocEncodingBuffer(Snappy
                .maxCompressedLength(blockSize));

        writeHeader(out);
    }

    /**
     * Writes the implementation specific header or "marker bytes" to
     * <i>out</i>.
     * 
     * @param out
     *            The underlying {@link OutputStream}.
     * @throws IOException
     */
    protected abstract void writeHeader(OutputStream out) throws IOException;

    @Override
    public void write(int b) throws IOException {
        if (closed) {
            throw new IOException("Stream is closed");
        }
        if (position >= blockSize) {
            flushBuffer();
        }
        buffer[position++] = (byte) b;
    }

    @Override
    public void write(byte[] input, int offset, int length) throws IOException {
        checkNotNull(input, "input is null");
        checkPositionIndexes(offset, offset + length, input.length);
        if (closed) {
            throw new IOException("Stream is closed");
        }

        int free = blockSize - position;

        // easy case: enough free space in buffer for entire input
        if (free >= length) {
            copyToBuffer(input, offset, length);
            return;
        }

        // fill partial buffer as much as possible and flush
        if (position > 0) {
            copyToBuffer(input, offset, free);
            flushBuffer();
            offset += free;
            length -= free;
        }

        // write remaining full blocks directly from input array
        while (length >= blockSize) {
            writeCompressed(input, offset, blockSize);
            offset += blockSize;
            length -= blockSize;
        }

        // copy remaining partial block into now-empty buffer
        copyToBuffer(input, offset, length);
    }

    @Override
    public final void flush() throws IOException {
        if (closed) {
            throw new IOException("Stream is closed");
        }
        flushBuffer();
        out.flush();
    }

    @Override
    public final void close() throws IOException {
        if (closed) {
            return;
        }
        try {
            flush();
            out.close();
        } finally {
            closed = true;
            recycler.releaseOutputBuffer(outputBuffer);
            recycler.releaseEncodeBuffer(buffer);
        }
    }

    private void copyToBuffer(byte[] input, int offset, int length) {
        System.arraycopy(input, offset, buffer, position, length);
        position += length;
    }

    /**
     * Compresses and writes out any buffered data. This does nothing if there
     * is no currently buffered data.
     * 
     * @throws IOException
     */
    private void flushBuffer() throws IOException {
        if (position > 0) {
            writeCompressed(buffer, 0, position);
            position = 0;
        }
    }

    /**
     * {@link #calculateCRC32C(byte[], int, int) Calculates} the crc, compresses
     * the data, determines if the compression ratio is acceptable and calls
     * {@link #writeBlock(OutputStream, byte[], int, int, boolean, int)} to
     * actually write the frame.
     * 
     * @param input
     *            The byte[] containing the raw data to be compressed.
     * @param offset
     *            The offset into <i>input</i> where the data starts.
     * @param length
     *            The amount of data in <i>input</i>.
     * @throws IOException
     */
    private void writeCompressed(byte[] input, int offset, int length)
            throws IOException {
        // crc is based on the user supplied input data
        int crc32c = calculateCRC32C(input, offset, length);

        int compressed = Snappy
                .compress(input, offset, length, outputBuffer, 0);

        // only use the compressed data if copmression ratio is <= the
        // minCompressonRatio
        if (((double) compressed / (double) length) <= minCompressionRatio) {
            writeBlock(out, outputBuffer, 0, compressed, true, crc32c);
        } else {
            // otherwise use the uncomprssed data.
            writeBlock(out, input, offset, length, false, crc32c);
        }
    }

    /**
     * Calculates a CRC32C checksum over the data.
     * <p>
     * This can be overridden to provider alternative implementations (such as
     * returning 0 if checksums are not desired).
     * </p>
     * 
     * @param data
     * @param offset
     * @param length
     * @return The CRC32 checksum.
     */
    protected int calculateCRC32C(byte[] data, int offset, int length) {
        return maskedCrc32c(data, offset, length);
    }

    /**
     * Write a frame (block) to <i>out</i>.
     * 
     * @param out
     *            The {@link OutputStream} to write to.
     * @param data
     *            The data to write.
     * @param offset
     *            The offset in <i>data</i> to start at.
     * @param length
     *            The length of <i>data</i> to use.
     * @param compressed
     *            Indicates if <i>data</i> is the compressed or raw content.
     *            This is based on whether the compression ratio desired is
     *            reached.
     * @param crc32c
     *            The calculated checksum.
     * @throws IOException
     */
    protected abstract void writeBlock(final OutputStream out, byte[] data,
            int offset, int length, boolean compressed, int crc32c)
            throws IOException;
}
