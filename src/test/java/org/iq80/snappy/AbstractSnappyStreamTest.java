/*
 * Created: Mar 14, 2013
 */
package org.iq80.snappy;

import static com.google.common.io.ByteStreams.toByteArray;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

import org.testng.annotations.Test;

import com.google.common.io.Files;

/**
 * Common base class for testing streaming implementations.
 * 
 * @author Brett Okken
 */
public abstract class AbstractSnappyStreamTest {
    protected abstract OutputStream createOutputStream(OutputStream target)
            throws IOException;

    protected abstract InputStream createInputStream(InputStream source,
            boolean verifyCheckSums) throws IOException;

    @Test
    public void testLargeWrites() throws Exception {
        byte[] random = getRandom(0.5, 500000);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        OutputStream snappyOut = createOutputStream(out);

        // partially fill buffer
        int small = 1000;
        snappyOut.write(random, 0, small);

        // write more than the buffer size
        snappyOut.write(random, small, random.length - small);

        // get compressed data
        snappyOut.close();
        byte[] compressed = out.toByteArray();
        assertTrue(compressed.length < random.length);

        // decompress
        byte[] uncompressed = uncompress(compressed);
        assertEquals(uncompressed, random);

        // decompress byte at a time
        InputStream in = createInputStream(
                new ByteArrayInputStream(compressed), true);
        int i = 0;
        int c;
        while ((c = in.read()) != -1) {
            uncompressed[i++] = (byte) c;
        }
        assertEquals(i, random.length);
        assertEquals(uncompressed, random);
    }

    @Test
    public void testSingleByteWrites() throws Exception {
        byte[] random = getRandom(0.5, 500000);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        OutputStream snappyOut = createOutputStream(out);

        for (byte b : random) {
            snappyOut.write(b);
        }

        snappyOut.close();
        byte[] compressed = out.toByteArray();
        assertTrue(compressed.length < random.length);

        byte[] uncompressed = uncompress(compressed);
        assertEquals(uncompressed, random);
    }

    @Test
    public void testExtraFlushes() throws Exception {
        byte[] random = getRandom(0.5, 500000);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        OutputStream snappyOut = createOutputStream(out);

        snappyOut.write(random);

        for (int i = 0; i < 10; i++) {
            snappyOut.flush();
        }

        snappyOut.close();
        byte[] compressed = out.toByteArray();
        assertTrue(compressed.length < random.length);

        byte[] uncompressed = uncompress(compressed);
        assertEquals(uncompressed, random);
    }

    @Test
    public void testUncompressableRange() throws Exception {
        int max = 128 * 1024;
        byte[] random = getRandom(1, max);

        for (int i = 1; i <= max; i += 102) {
            byte[] original = Arrays.copyOfRange(random, 0, i);

            byte[] compressed = compress(original);
            byte[] uncompressed = uncompress(compressed);

            assertEquals(uncompressed, original);
            // assertEquals(compressed.length, original.length + overhead);
        }
    }

    @Test
    public void testByteForByteTestData() throws Exception {
        for (File testFile : SnappyTest.getTestFiles()) {
            byte[] original = Files.toByteArray(testFile);
            byte[] compressed = compress(original);
            byte[] uncompressed = uncompress(compressed);
            assertEquals(uncompressed, original);
        }
    }

    @Test(expectedExceptions = EOFException.class, expectedExceptionsMessageRegExp = ".*stream header.*")
    public void testEmptyStream() throws Exception {
        uncompress(new byte[0]);
    }

    @Test(expectedExceptions = IOException.class, expectedExceptionsMessageRegExp = ".*stream header.*")
    public void testInvalidStreamHeader() throws Exception {
        uncompress(new byte[] { 'b', 0, 0, 'g', 'u', 's', 0 });
    }

    @Test
    public void testCloseIsIdempotent() throws Exception {
        byte[] random = getRandom(0.5, 500000);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        OutputStream snappyOut = createOutputStream(out);

        snappyOut.write(random);

        snappyOut.close();
        snappyOut.close();

        byte[] compressed = out.toByteArray();

        InputStream snappyIn = createInputStream(new ByteArrayInputStream(
                compressed), true);
        byte[] uncompressed = toByteArray(snappyIn);
        assertEquals(uncompressed, random);

        snappyIn.close();
        snappyIn.close();
    }

    /**
     * Tests that the presence of the marker bytes can appear as a valid frame
     * anywhere in stream.
     * 
     * @throws IOException
     */
    @Test
    public void testMarkerFrameInStream() throws IOException {
        final int size = 500000;
        byte[] random = getRandom(0.5, size);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        final OutputStream os = createOutputStream(out);

        final byte[] markerFrame = getMarkerFrame();

        for (int i = 0; i < size;) {
            int toWrite = Math.max((size - i) / 4, 512);

            // write some data to be compressed
            os.write(random, i, Math.min(size - i, toWrite));
            // force the write of a frame
            os.flush();

            // write the marker frame to the underlying byte array output stream
            out.write(markerFrame);

            // this is not accurate for the final write, but at that point it
            // does not matter
            // as we will be exiting the for loop now
            i += toWrite;
        }

        final byte[] compressed = out.toByteArray();
        final byte[] uncompressed = uncompress(compressed);

        assertEquals(random, uncompressed);
    }

    protected abstract byte[] getMarkerFrame();

    protected static byte[] getRandom(double compressionRatio, int length) {
        SnappyTest.RandomGenerator gen = new SnappyTest.RandomGenerator(
                compressionRatio);
        gen.getNextPosition(length);
        byte[] random = Arrays.copyOf(gen.data, length);
        assertEquals(random.length, length);
        return random;
    }

    protected byte[] compress(byte[] original) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        OutputStream snappyOut = createOutputStream(out);
        snappyOut.write(original);
        snappyOut.close();
        return out.toByteArray();
    }

    protected byte[] uncompress(byte[] compressed) throws IOException {
        return toByteArray(createInputStream(new ByteArrayInputStream(
                compressed), true));
    }
}
