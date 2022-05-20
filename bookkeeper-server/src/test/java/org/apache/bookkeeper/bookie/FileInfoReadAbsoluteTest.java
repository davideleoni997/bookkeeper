package org.apache.bookkeeper.bookie;


import org.junit.Test;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import  org.junit.Assert;


import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import java.util.Arrays;
import java.util.Collection;


@RunWith(value= Parameterized.class)
public class FileInfoReadAbsoluteTest {
    FileInfo fi;
    private ByteBuffer bb;
    private long position;
    private boolean bestEffort;
    private long read;


    @Parameterized.Parameters
    public static Collection<Object[]> testParameters(){
        return Arrays.asList(new Object[][]{
                {ByteBuffer.allocate(6),0,true,"header",6,true},
                {ByteBuffer.allocate(6),0,true,"",0,true},
                {ByteBuffer.allocate(6),-1,true,"",0,true},
                {null,0,true,"data",0,true},
                {ByteBuffer.allocate(0),0,true,"hello",0,true},
                {ByteBuffer.allocate(6),0,false,"false",0,true},
                {ByteBuffer.allocate(6),0,true,"false",5,false},

        });
    }

    public FileInfoReadAbsoluteTest(ByteBuffer bb, long position, boolean bestEffort, String data, long read, boolean fc) throws IOException {
        configure(bb, position,bestEffort,data,read,fc);
    }

    private void configure(ByteBuffer bb, long position,boolean bestEffort,String data,long read,boolean fc) throws IOException {
        File basedir = File.createTempFile("test","file");
        FileInfo fi = new FileInfo(basedir,"1".getBytes(),0);
        ByteBuffer[] arr = {ByteBuffer.wrap(data.getBytes())};

        fi.write(arr,0);

        if (!fc){
            fi.close(false);
        }

        this.bb = bb;
        this.position = position;
        this.bestEffort = bestEffort;
        this.fi=fi;
        this.read = read;
    }



    @Test
    public void test_readAbsol() throws IOException {

        if(bb!=null) {
            if (bestEffort)
                Assert.assertEquals(read, fi.read(bb, position, bestEffort));
            else
                Assert.assertThrows(ShortReadException.class, () -> {
                    fi.read(bb, position, bestEffort);
                });
        }
        else Assert.assertThrows(NullPointerException.class,() ->{
            fi.read(bb,position,bestEffort);
        });
    }
}
