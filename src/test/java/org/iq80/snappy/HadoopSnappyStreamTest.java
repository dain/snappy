/*
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

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static org.apache.hadoop.fs.CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_DEFAULT;

public class HadoopSnappyStreamTest
        extends AbstractSnappyStreamTest
{
    @Override
    protected OutputStream createOutputStream(OutputStream target)
    {
        return new HadoopSnappyOutputStream(target, IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_DEFAULT);
    }

    @Override
    protected InputStream createInputStream(InputStream source, boolean verifyCheckSums)
            throws IOException
    {
        return new HadoopSnappyInputStream(source);
    }

    @Test
    public void testEmptyStream()
            throws Exception
    {
        Assert.assertEquals(uncompress(new byte[0]), new byte[0]);
    }
}
