/*
 * Created: Mar 14, 2013
 */
package org.iq80.snappy;

import static com.google.common.io.ByteStreams.toByteArray;
import static com.google.common.primitives.UnsignedBytes.toInt;
import static org.iq80.snappy.SnappyFramed.COMPRESSED_DATA_FLAG;
import static org.iq80.snappy.SnappyFramed.HEADER_BYTES;
import static org.iq80.snappy.SnappyFramed.UNCOMPRESSED_DATA_FLAG;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.google.common.base.Charsets;

/**
 * Tests the functionality of {@link org.iq80.snappy.SnappyFramedInputStream}
 * and {@link org.iq80.snappy.SnappyFramedOutputStream}.
 * 
 * @author Brett Okken
 */
public class SnappyFramedStreamTest extends AbstractSnappyStreamTest {

    @BeforeTest
    @AfterTest
    public void resetBufferRecycler() {
        BufferRecycler.instance().clear();
    }

    /**
     * {@inheritDoc}
     * 
     * @throws IOException
     */
    @Override
    protected OutputStream createOutputStream(OutputStream target)
            throws IOException {
        return new SnappyFramedOutputStream(target);
    }

    /**
     * {@inheritDoc}
     * 
     * @throws IOException
     */
    @Override
    protected InputStream createInputStream(InputStream source,
            boolean verifyCheckSums) throws IOException {
        return new SnappyFramedInputStream(source, verifyCheckSums);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected byte[] getMarkerFrame() {
        return HEADER_BYTES;
    }

    @Test
    public void testSimple() throws Exception {
        byte[] original = "aaaaaaaaaaaabbbbbbbaaaaaa".getBytes(Charsets.UTF_8);

        byte[] compressed = compress(original);
        byte[] uncompressed = uncompress(compressed);

        assertEquals(uncompressed, original);
        // 10 byte stream header, 4 byte block header, 4 byte crc, 19 bytes
        assertEquals(compressed.length, 37);

        // stream header
        assertEquals(Arrays.copyOf(compressed, 10), HEADER_BYTES);

        // flag: compressed
        assertEquals(toInt(compressed[10]), COMPRESSED_DATA_FLAG);

        // length: 23 = 0x000017
        assertEquals(toInt(compressed[11]), 0x17);
        assertEquals(toInt(compressed[12]), 0x00);
        assertEquals(toInt(compressed[13]), 0x00);

        // crc32c: 0x9274cda8
        assertEquals(toInt(compressed[17]), 0x92);
        assertEquals(toInt(compressed[16]), 0x74);
        assertEquals(toInt(compressed[15]), 0xCD);
        assertEquals(toInt(compressed[14]), 0xA8);
    }

    @Test
    public void testUncompressable() throws Exception {
        byte[] random = getRandom(1, 5000);
        int crc32c = Crc32C.maskedCrc32c(random);

        byte[] compressed = compress(random);
        byte[] uncompressed = uncompress(compressed);

        assertEquals(uncompressed, random);
        assertEquals(compressed.length, random.length + 10 + 4 + 4);

        // flag: uncompressed
        assertEquals(toInt(compressed[10]), UNCOMPRESSED_DATA_FLAG);

        // length: 5004 = 0x138c
        assertEquals(toInt(compressed[13]), 0x00);
        assertEquals(toInt(compressed[12]), 0x13);
        assertEquals(toInt(compressed[11]), 0x8c);
    }

    @Test
    public void testEmptyCompression() throws Exception {
        byte[] empty = new byte[0];
        assertEquals(compress(empty), HEADER_BYTES);
        assertEquals(uncompress(HEADER_BYTES), empty);
    }

    @Test(expectedExceptions = EOFException.class, expectedExceptionsMessageRegExp = ".*block header.*")
    public void testShortBlockHeader() throws Exception {
        uncompressBlock(new byte[] { 0 });
    }

    @Test(expectedExceptions = EOFException.class, expectedExceptionsMessageRegExp = ".*reading frame.*")
    public void testShortBlockData() throws Exception {
        // flag = 0, size = 8, crc32c = 0, block data= [x, x]
        uncompressBlock(new byte[] { 1, 8, 0, 0, 0, 0, 0, 0, 'x', 'x' });
    }

    @Test
    public void testUnskippableChunkFlags() throws Exception {
        for (int i = 2; i <= 0x7f; i++) {
            try {
                uncompressBlock(new byte[] { (byte) i, 5, 0, 0, 0, 0, 0, 0, 0 });
                fail("no exception thrown with flag: " + Integer.toHexString(i));
            } catch (IOException e) {

            }
        }
    }

    @Test
    public void testSkippableChunkFlags() throws Exception {
        for (int i = 0x80; i <= 0xfe; i++) {
            try {
                uncompressBlock(new byte[] { (byte) i, 5, 0, 0, 0, 0, 0, 0, 0 });
            } catch (IOException e) {
                fail("exception thrown with flag: " + Integer.toHexString(i), e);
            }
        }
    }

    @Test(expectedExceptions = IOException.class, expectedExceptionsMessageRegExp = "invalid length.*4.*")
    public void testInvalidBlockSizeZero() throws Exception {
        // flag = '0', block size = 4, crc32c = 0
        uncompressBlock(new byte[] { 1, 4, 0, 0, 0, 0, 0, 0 });
    }

    @Test(expectedExceptions = IOException.class, expectedExceptionsMessageRegExp = "Corrupt input: invalid checksum")
    public void testInvalidChecksum() throws Exception {
        // flag = 0, size = 5, crc32c = 0, block data = [a]
        uncompressBlock(new byte[] { 1, 5, 0, 0, 0, 0, 0, 0, 'a' });
    }

    @Test
    public void testInvalidChecksumIgnoredWhenVerificationDisabled()
            throws Exception {
        // flag = 0, size = 4, crc32c = 0, block data = [a]
        byte[] block = { 1, 5, 0, 0, 0, 0, 0, 0, 'a' };
        ByteArrayInputStream inputData = new ByteArrayInputStream(
                blockToStream(block));
        assertEquals(toByteArray(createInputStream(inputData, false)),
                new byte[] { 'a' });
    }

    @Test
    public void testLargerFrames_raw_() throws IOException {
        final byte[] random = getRandom(0.5, 100000);

        final byte[] stream = new byte[HEADER_BYTES.length + 8 + random.length];
        System.arraycopy(HEADER_BYTES, 0, stream, 0, HEADER_BYTES.length);

        stream[10] = UNCOMPRESSED_DATA_FLAG;
        
        int length = random.length + 4;
        stream[11] = (byte) length;
        stream[12] = (byte) (length >>> 8);
        stream[13] = (byte) (length >>> 16);

        int crc32c = Crc32C.maskedCrc32c(random);
        stream[14] = (byte) crc32c;
        stream[15] = (byte) (crc32c >>> 8);
        stream[16] = (byte) (crc32c >>> 16);
        stream[17] = (byte) (crc32c >>> 24);

        System.arraycopy(random, 0, stream, 18, random.length);

        final byte[] uncompressed = uncompress(stream);

        assertEquals(random, uncompressed);
    }

    @Test
    public void testLargerFrames_compressed_() throws IOException {
        final byte[] random = getRandom(0.5, 500000);

        final byte[] compressed = Snappy.compress(random);
                
        final byte[] stream = new byte[HEADER_BYTES.length + 8 + compressed.length];
        System.arraycopy(HEADER_BYTES, 0, stream, 0, HEADER_BYTES.length);

        stream[10] = COMPRESSED_DATA_FLAG;
        
        int length = compressed.length + 4;
        stream[11] = (byte) length;
        stream[12] = (byte) (length >>> 8);
        stream[13] = (byte) (length >>> 16);

        int crc32c = Crc32C.maskedCrc32c(random);
        stream[14] = (byte) crc32c;
        stream[15] = (byte) (crc32c >>> 8);
        stream[16] = (byte) (crc32c >>> 16);
        stream[17] = (byte) (crc32c >>> 24);

        System.arraycopy(compressed, 0, stream, 18, compressed.length);

        final byte[] uncompressed = uncompress(stream);

        assertEquals(random, uncompressed);
    }

    @Test
    public void testLargerFrames_compressed_smaller_raw_larger()
            throws IOException {
        final byte[] random = getRandom(0.5, 100000);

        final byte[] compressed = Snappy.compress(random);

        final byte[] stream = new byte[HEADER_BYTES.length + 8
                + compressed.length];
        System.arraycopy(HEADER_BYTES, 0, stream, 0, HEADER_BYTES.length);

        stream[10] = COMPRESSED_DATA_FLAG;
        
        int length = compressed.length + 4;
        stream[11] = (byte) length;
        stream[12] = (byte) (length >>> 8);
        stream[13] = (byte) (length >>> 16);

        int crc32c = Crc32C.maskedCrc32c(random);
        stream[14] = (byte) crc32c;
        stream[15] = (byte) (crc32c >>> 8);
        stream[16] = (byte) (crc32c >>> 16);
        stream[17] = (byte) (crc32c >>> 24);

        System.arraycopy(compressed, 0, stream, 18, compressed.length);

        final byte[] uncompressed = uncompress(stream);

        assertEquals(random, uncompressed);
    }

    private byte[] uncompressBlock(byte[] block) throws IOException {
        return uncompress(blockToStream(block));
    }

    private static byte[] blockToStream(byte[] block) {
        byte[] stream = new byte[HEADER_BYTES.length + block.length];
        System.arraycopy(HEADER_BYTES, 0, stream, 0, HEADER_BYTES.length);
        System.arraycopy(block, 0, stream, HEADER_BYTES.length, block.length);
        return stream;
    }
}
