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

import com.google.common.io.Files;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Random;

public class SnappyTest
{
    private static final File TEST_DATA_DIR = new File("testdata");
    private final RandomGenerator randomGenerator = new RandomGenerator(0.5);

    @Test
    public void testNativeCompatibleSyntheticData()
            throws Exception
    {
        for (int i = 1; i < 65 * 1024; i++) {
            try {
                verifyCompression(i);
            }
            catch (Error e) {
                e.printStackTrace();
                Assert.fail(i + " byte block", e);
            }
        }
    }

    @Test
    public void testNativeCompatibleTestData()
            throws Exception
    {
        for (File testFile : getTestFiles()) {
            byte[] data = Files.toByteArray(testFile);
            try {
                verifyCompression(data, 0, data.length);
            }
            catch (Throwable e) {
                e.printStackTrace();
                Assert.fail("Testdata: " + testFile.getName(), e);
            }
        }
    }

    @Test(expectedExceptions = CorruptionException.class, expectedExceptionsMessageRegExp = "Malformed input: offset=5")
    public void testInvalidLiteralLength()
    {
        byte[] data = {
                // Encoded uncompressed length 1024
                -128, 8,
                // op-code
                (byte) 252,
                // Trailer value Integer.MAX_VALUE
                (byte) 0b1111_1111, (byte) 0b1111_1111, (byte) 0b1111_1111, (byte) 0b0111_1111,
                // Some arbitrary data
                0, 0, 0, 0, 0, 0, 0, 0
        };

        Snappy.uncompress(data, 0, data.length, new byte[1024], 0, 1024);
    }

    @Test(expectedExceptions = CorruptionException.class, expectedExceptionsMessageRegExp = "negative compressed length: offset=16")
    public void testNegativeLength()
    {
        byte[] data = {(byte) 255, (byte) 255, (byte) 255, (byte) 255, 0b0000_1000};
        Snappy.getUncompressedLength(data, 0);
    }

    private void verifyCompression(int size)
            throws Exception
    {
        byte[] input = randomGenerator.data;
        int position = randomGenerator.getNextPosition(size);

        verifyCompression(input, position, size);
    }

    private static void verifyCompression(byte[] input, int position, int size)
            throws Exception
    {
        byte[] javaCompressed = new byte[Snappy.maxCompressedLength(size)];

        int javaCompressedSize = Snappy.compress(
                new Snappy.CompressionContext(),
                input,
                position,
                size,
                javaCompressed,
                0,
                javaCompressed.length);

        // Verify Java codec decompresses Java compressed data
        byte[] uncompressed = new byte[size];
        int uncompressedSize = Snappy.uncompress(javaCompressed, 0, javaCompressedSize, uncompressed, 0);
        Assert.assertEquals(uncompressedSize, size, "Size mismatch");
        Assert.assertTrue(arraysEqual(input, position, uncompressed, 0, size), "Data mismatch");

        // Verify Native codec decompresses Java compressed data
        byte[] nativeUncompressed = new byte[size];
        int nativeUncompressedSize = org.xerial.snappy.Snappy.uncompress(
                javaCompressed,
                0,
                javaCompressedSize,
                nativeUncompressed,
                0);
        Assert.assertEquals(nativeUncompressedSize, size, "Size mismatch");
        Assert.assertTrue(arraysEqual(input, position, nativeUncompressed, 0, size), "Data mismatch");

        // Verify Java codec decompresses Native compressed data
        byte[] nativeCompressed = new byte[org.xerial.snappy.Snappy.maxCompressedLength(size)];
        int nativeCompressedSize = org.xerial.snappy.Snappy.compress(
                input,
                position,
                size,
                nativeCompressed,
                0);
        uncompressedSize = Snappy.uncompress(nativeCompressed, 0, nativeCompressedSize, uncompressed, 0);
        Assert.assertEquals(uncompressedSize, size, "Size mismatch");
        Assert.assertTrue(arraysEqual(input, position, uncompressed, 0, size), "Data mismatch");
    }

    public static class RandomGenerator
    {
        public final byte[] data;
        public int position;

        public RandomGenerator(double compressionRatio)
        {
            // We use a limited amount of data over and over again and ensure
            // that it is larger than the compression window (32KB), and also
            // large enough to serve all typical value sizes we want to write.
            Random rnd = new Random(301);
            data = new byte[1048576 + 100];
            for (int i = 0; i < 1048576; i += 100) {
                // Add a short fragment that is as compressible as specified ratio
                System.arraycopy(compressibleData(rnd, compressionRatio, 100), 0, data, i, 100);
            }
        }

        public int getNextPosition(int length)
        {
            if (position + length > data.length) {
                position = 0;
                assert (length < data.length);
            }
            int result = position;
            position += length;
            return result;
        }

        private static byte[] compressibleData(Random random, double compressionRatio, int length)
        {
            int raw = (int) (length * compressionRatio);
            if (raw < 1) {
                raw = 1;
            }
            byte[] rawData = generateRandomData(random, raw);

            // Duplicate the random data until we have filled "length" bytes
            byte[] dest = new byte[length];
            for (int i = 0; i < length; ) {
                int chunkLength = Math.min(rawData.length, length - i);
                System.arraycopy(rawData, 0, dest, i, chunkLength);
                i += chunkLength;
            }
            return dest;
        }

        private static byte[] generateRandomData(Random random, int length)
        {
            byte[] rawData = new byte[length];
            for (int i = 0; i < rawData.length; i++) {
                rawData[i] = (byte) random.nextInt(256);
            }
            return rawData;
        }
    }

    static File[] getTestFiles()
    {
        File[] testFiles = TEST_DATA_DIR.listFiles();
        Assert.assertTrue(testFiles != null && testFiles.length > 0, "No test files at " + TEST_DATA_DIR.getAbsolutePath());
        return testFiles;
    }

    private static boolean arraysEqual(byte[] left, int leftIndex, byte[] right, int rightIndex, int length)
    {
        for (int i = 0; i < length; i++) {
            if (left[leftIndex + i] != right[rightIndex + i]) {
                return false;
            }
        }
        return true;
    }
}
