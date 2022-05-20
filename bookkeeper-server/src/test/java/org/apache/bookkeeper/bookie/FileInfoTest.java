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
public class FileInfoTest {
    private FileInfo fi;
    private File newFile;
    private long size;

    private File oldFile;


    @Parameterized.Parameters
    public static Collection<Object[]> testParameters() throws IOException {
        File baseFile = File.createTempFile("test","file");
        return Arrays.asList(new Object[][]{
                //{baseFile,File.createTempFile("new","file"),Long.MAX_VALUE},
                //{baseFile,File.createTempFile("new","file"),0},
                {baseFile,File.createTempFile("new","file"),-1},
                {baseFile,null,Long.MAX_VALUE},
                {baseFile,null,0},
                {baseFile,baseFile,0},

        });
    }

    public FileInfoTest(File baseFile, File newFile,long size) throws IOException {
        configure(baseFile,newFile,size);
    }

    private void configure(File basedir, File newFile,long size) throws IOException {
        FileInfo fi = new FileInfo(basedir,"1".getBytes(),0);


        this.newFile = newFile;
        this.size = size;
        this.fi = fi;
        this.oldFile = fi.getLf();

    }

    @Test
    public void test_moveToNew() throws IOException {

        if(newFile != oldFile)
            if(newFile == null)
                Assert.assertThrows(NullPointerException.class,()->{
                    fi.moveToNewLocation(newFile,size);
                });
            else {
                fi.moveToNewLocation(newFile,size);
                Assert.assertNotEquals(oldFile, fi.getLf());
            }
        else {
            fi.moveToNewLocation(newFile,size);
            Assert.assertEquals(oldFile, fi.getLf());
        }
    }
}
