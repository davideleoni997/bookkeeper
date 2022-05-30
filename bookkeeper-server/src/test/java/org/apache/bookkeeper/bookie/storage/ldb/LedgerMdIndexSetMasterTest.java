package org.apache.bookkeeper.bookie.storage.ldb;


import com.google.protobuf.ByteString;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.mockito.Mock;
import org.mockito.internal.matchers.Null;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;


@RunWith(value = Parameterized.class)
public class LedgerMdIndexSetMasterTest {

    @Mock
    private KeyValueStorageFactory fact = mock(KeyValueStorageFactory.class);
    @Mock
    private KeyValueStorage keyValueStorage = mock(KeyValueStorage.class);
    @Mock
    private KeyValueStorage.CloseableIterator cI = mock(KeyValueStorage.CloseableIterator.class);

    private Iterator<Map.Entry<byte[], byte[]>> iter;

    private long id;
    private byte[] key;
    private LedgerMetadataIndex lmi;

    @Parameterized.Parameters
    public static Collection<Object[]> testParameters(){

        return Arrays.asList(new Object[][]{
                {-1,"".getBytes(),false},
                {1,"key".getBytes(),false},
                {1,"".getBytes(),false},
                {1,null,false},
                {1,"key".getBytes(),true},
                {1,"chiave".getBytes(),true},
        });
    }

    public LedgerMdIndexSetMasterTest(long id, byte[] key,boolean isKeySet) throws IOException {
        configure(id, key, isKeySet);
    }

    private void configure(long ledgerId, byte[] key, boolean isKeySet) throws IOException {
        //Since we don't want to create ledgers and the system behind the metadataIndex we will mock the external system
        ServerConfiguration sconf = new ServerConfiguration();//whatever server config
        HashMap<byte[],byte[]> ledger = new HashMap<>();//This will be our ledger, as per example in row 82 of LedgerMetadataIndex.java

        ByteBuffer buf = ByteBuffer.allocate(8);
        buf.putLong(ledgerId); //make the id a byte array to use as a key
        if(isKeySet)
            ledger.put(buf.array(),DbLedgerStorageDataFormats.LedgerData.newBuilder().setExists(true).setFenced(false).setMasterKey(ByteString.copyFrom("chiave".getBytes())).build().toByteArray());
        else// 0 bytes array
            ledger.put(buf.array(),DbLedgerStorageDataFormats.LedgerData.newBuilder().setExists(true).setFenced(false).setMasterKey(ByteString.copyFrom(new byte[]{0,0})).build().toByteArray());


        when(this.fact.newKeyValueStorage(any(), any(), any(),any())).thenReturn(this.keyValueStorage);//We return our own mocked implementation of a keyvalue storage, without using the external factory
        when(this.keyValueStorage.iterator()).then(invocationOnMock -> { // if we ask for the keyvaluestorage iterator we return our own(row 79)
            this.iter = ledger.entrySet().iterator(); //equal our iterator to the one of the ledger
            return this.cI;
        });
        when(this.cI.hasNext()).then(invocationOnMock -> this.iter.hasNext()); //if we call the methods of the closeableIterator use the ones from our iterator
        when(this.cI.next()).then(invocationOnMock -> this.iter.next());

        NullStatsLogger log = new NullStatsLogger(); //We don't log any stat

        this.lmi = new LedgerMetadataIndex(sconf, this.fact, "tmp",log);//create a ledger with basePath in tmp
        this.id = ledgerId;
        this.key = key;
    }

    @Test
    public void testSetMaster(){
       try {
           lmi.setMasterKey(this.id, this.key);
           Assert.assertEquals(ByteBuffer.wrap(this.key), ByteBuffer.wrap(lmi.get(this.id).getMasterKey().toByteArray()));
       } catch (Exception e){
           if(this.key == null)
                Assert.assertEquals(e.getClass(),NullPointerException.class);
           else
               Assert.assertEquals(e.getClass(), IOException.class);
       }


    }

}
