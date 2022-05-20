package org.apache.bookkeeper.bookie;


import org.junit.After;
import org.junit.Test;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import  org.junit.Assert;


import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import java.util.Arrays;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;


@RunWith(value= Parameterized.class)
public class FileInfoTest {
    private FileInfo fi;
    private File newFile;
    private long size;

    private File oldFile;

    @Parameterized.Parameters
    public static Collection<Object[]> testParameters() throws IOException {
        File baseFileEqual = File.createTempFile("test","file");
        return Arrays.asList(new Object[][]{
                {File.createTempFile("first","file"),File.createTempFile("new","stuff"),Long.MAX_VALUE},
                {File.createTempFile("second","file"),File.createTempFile("another","thing"),0},
                {File.createTempFile("third","file"),File.createTempFile("third","coso"),60},
                {File.createTempFile("fourth","file"),null,Long.MAX_VALUE},
                {File.createTempFile("fifth","test"),null,0},
               {baseFileEqual,baseFileEqual,Long.MAX_VALUE},

        });
    }

    public FileInfoTest(File baseFile, File newFile,long size) throws IOException {
        configure(baseFile,newFile,size);
    }

    private void configure(File basedir, File newFile,long size) throws IOException {
        FileInfo fi = new FileInfo(basedir,"1".getBytes(),0);

        if(newFile != null && newFile.getName().contains("third"))
            newFile.setReadOnly();

        this.newFile = newFile;
        this.size = size;
        this.fi = fi;
        this.oldFile = fi.getLf();

    }

    @Test
    public void test_moveToNew() throws IOException {
    Logger.getGlobal().log(Level.INFO,"Base " + fi.getLf().getName());
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
