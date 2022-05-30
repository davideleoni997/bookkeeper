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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;


@RunWith(value = Parameterized.class)
public class LedgerMdIndexSetTest {

    @Mock
    private KeyValueStorageFactory fact = mock(KeyValueStorageFactory.class);
    @Mock
    private KeyValueStorage keyValueStorage = mock(KeyValueStorage.class);
    @Mock
    private KeyValueStorage.CloseableIterator cI = mock(KeyValueStorage.CloseableIterator.class);

    private Iterator<Map.Entry<byte[], byte[]>> iter;

    private long id;
    private DbLedgerStorageDataFormats.LedgerData data;
    private LedgerMetadataIndex lmi;

    @Parameterized.Parameters
    public static Collection<Object[]> testParameters(){
        //Create new LedgerData as shown in setMasterKey
        DbLedgerStorageDataFormats.LedgerData data = DbLedgerStorageDataFormats.LedgerData.newBuilder().setExists(true).setFenced(false).setMasterKey(ByteString.copyFrom(ByteBuffer.wrap("Chiave".getBytes()))).build();
        return Arrays.asList(new Object[][]{
                {data,false},
                {null,false},
                {data,true},


        });
    }

    public LedgerMdIndexSetTest(DbLedgerStorageDataFormats.LedgerData data,boolean notPresent) throws IOException {
        configure(data,notPresent);
    }

    private void configure(DbLedgerStorageDataFormats.LedgerData data,boolean notPresent) throws IOException {
        //Since we don't want to create ledgers and the system behind the metadataIndex we will mock the external system
        ServerConfiguration sconf = new ServerConfiguration();//whatever server config
        HashMap<byte[],byte[]> ledger = new HashMap<>();//This will be our ledger, as per example in row 82 of LedgerMetadataIndex.java

        ByteBuffer buf = ByteBuffer.allocate(8);
        buf.putLong(1); //make the id a byte array to use as a key

        if(notPresent){
            ledger.put(buf.array(), data.toByteArray());
        }

        when(this.fact.newKeyValueStorage(any(), any(), any(),any())).thenReturn(this.keyValueStorage);//We return our own mocked implementation of a keyvalue storage, without using the external factory
        when(this.keyValueStorage.iterator()).then(invocationOnMock -> { // if we ask for the keyvaluestorage iterator we return our own(row 79)
            this.iter = ledger.entrySet().iterator(); //equal our iterator to the one of the ledger
            return this.cI;
        });
        when(this.cI.hasNext()).then(invocationOnMock -> this.iter.hasNext()); //if we call the methods of the closeableIterator use the ones from our iterator
        when(this.cI.next()).then(invocationOnMock -> this.iter.next());

        NullStatsLogger log = new NullStatsLogger(); //We don't log any stat

        this.lmi = new LedgerMetadataIndex(sconf, this.fact, "tmp",log);//create a ledger with basePath in tmp
        this.id = 1;
        this.data = data;
    }

    @Test
    public void testSetMethod() throws IOException {
        try {
            lmi.set(this.id, this.data);
            Assert.assertEquals(this.data, lmi.get(this.id));
        }
        catch (Exception e){
            Assert.assertEquals(e.getClass(),NullPointerException.class);
        }
    }

}
