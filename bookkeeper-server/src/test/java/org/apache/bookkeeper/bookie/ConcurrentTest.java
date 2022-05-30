
package org.apache.bookkeeper.bookie;


import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import  org.junit.Assert;


import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.logging.Logger;


    @RunWith(value= Parameterized.class)
    public class ConcurrentTest {
        FileInfo fi;
        private ByteBuffer bb;
        private long position;
        private boolean bestEffort;
        private long read;
        private boolean fc;


        @Parameterized.Parameters
        public static Collection<Object[]> testParameters() {
            return Arrays.asList(new Object[][]{
                    //{ByteBuffer.allocate(200), 0, true, "header", 12, true},
                    //{ByteBuffer.allocate(6),0,true,"data",0,false}


            });
        }

        public ConcurrentTest(ByteBuffer bb, long position, boolean bestEffort, String data, long read, boolean fc) throws IOException {
            configure(bb, position, bestEffort, data, read, fc);
        }

        private void configure(ByteBuffer bb, long position, boolean bestEffort, String data, long read, boolean fc) throws IOException {
            File basedir = File.createTempFile("test", "file");
            FileInfo fi = new FileInfo(basedir, "1".getBytes(), 0);
            ByteBuffer[] arr = {ByteBuffer.wrap(data.getBytes())};

            fi.write(arr, 0);

            if (!fc) {
                fi.close(true);
            }

            this.bb = bb;
            this.position = position;
            this.bestEffort = bestEffort;
            this.fi = fi;
            this.read = read;
            this.fc = fc;
        }

        @Test
        public void test_readAbsol_conc() throws IOException {

            Runnable readThread = () -> {
                try {
                    Assert.assertEquals(read,fi.read(bb,position,bestEffort));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            };

            Runnable writeThread= () -> {
                try {if(fc)
                    fi.write(new ByteBuffer[]{ByteBuffer.wrap("letter".getBytes())},0);
                    else
                        fi.close(true);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            };

            readThread.run();
            writeThread.run();
        }

    }