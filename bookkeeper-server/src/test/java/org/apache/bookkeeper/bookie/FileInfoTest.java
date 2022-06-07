package org.apache.bookkeeper.bookie;



import org.junit.Test;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import  org.junit.Assert;



import java.io.File;
import java.io.FileWriter;
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
    private boolean notReadable;

    @Parameterized.Parameters
    public static Collection<Object[]> testParameters() throws IOException {
        File baseFileEqual = File.createTempFile("test","file");

        return Arrays.asList(new Object[][]{
                {File.createTempFile("first","file"),File.createTempFile("new","stuff"),Long.MAX_VALUE,false,false,false},
                {File.createTempFile("second","file"),File.createTempFile("another","thing"),0,false,false,false},
                {File.createTempFile("third","file"),File.createTempFile("third","coso"),10,false,false,false},
                {File.createTempFile("fourth","file"),File.createTempFile("fourth","coso"),-1,false,false,false},
                {File.createTempFile("fifth","file"),null,Long.MAX_VALUE,false,false,false},
                {baseFileEqual,baseFileEqual,Long.MAX_VALUE,false,false,false},
                {File.createTempFile("sixth","file"),File.createTempFile("another","another"),Long.MAX_VALUE,true,false,false},
                {File.createTempFile("seventh","file"),File.createTempFile("another","file"),Long.MAX_VALUE,true,true,false},
                {File.createTempFile("eight","file"),File.createTempFile("file","file"),Long.MAX_VALUE,false,false,true},

        });
    }

    public FileInfoTest(File baseFile, File newFile,long size,boolean existRloc,boolean notReadable, boolean close) throws IOException {
        configure(baseFile,newFile,size,existRloc,notReadable,close);
    }

    private void configure(File basedir, File newFile,long size,boolean existRloc,boolean notReadable,boolean close) throws IOException {
        FileInfo fi = new FileInfo(basedir,"1".getBytes(),0);
        File rlocFile = null;
        if(basedir!= null)
            fi.write(new ByteBuffer[]{ByteBuffer.wrap("SomeRandomDataInTheBasedirFile".getBytes())},0);

        if(existRloc) {
            if(newFile!=null) {
                rlocFile = new File(newFile.getParentFile(), newFile.getName() + IndexPersistenceMgr.RLOC);
                FileWriter write = new FileWriter(rlocFile);
                write.write("randomStringforthefile");
                write.close();
            }
        }
        if(notReadable){
            if(rlocFile!=null) {
                 rlocFile.setReadable(false,false);
            }
        }
        if(close)
            fi.close(true);

        this.newFile = newFile;
        this.size = size;
        this.fi = fi;
        this.oldFile = fi.getLf();
        this.notReadable = notReadable;

    }

    @Test
    public void test_moveToNew() throws IOException {

        if(notReadable)
            Assert.assertThrows(IOException.class,()->{
              fi.moveToNewLocation(newFile,size);
            });
        else
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
