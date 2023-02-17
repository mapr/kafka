package org.apache.kafka.mapr.tools;

import com.mapr.fs.MapRFileAce;
import com.mapr.fs.MapRFileSystem;
import com.mapr.security.UnixUserGroupHelper;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.KafkaException;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(PowerMockRunner.class)
@SuppressStaticInitializationFor({"com.mapr.fs.MapRFileSystem","com.mapr.baseutils.JVMProperties"})
public class KafkaMaprfsTest extends EasyMockSupport {
    private static final String FILE = "/user/mapr/file";
    private static final Path FOLDER_PATH = new Path(FILE);
    private static final String MAPR_USER_1 = "mapruser1";
    private static final String MAPR_ADMIN = "mapr";
    private static final String UNKNOWN_USER = "unknown-user";
    private MapRFileSystem delegatedFs;
    private KafkaMaprfs maprfs;

    @Before
    public void setUp() throws IllegalAccessException {
        SuppressionUtil.clearDefaultResources();
        KafkaMaprTools.tools = mock(KafkaMaprTools.class);
        delegatedFs = mock(MapRFileSystem.class);
        maprfs = new KafkaMaprfs(delegatedFs);
        UnixUserGroupHelper unixUserGroupHelper = EasyMock.mock(UnixUserGroupHelper.class);
        expect(unixUserGroupHelper.getUserId(MAPR_USER_1)).andStubReturn(1001);
        expect(unixUserGroupHelper.getUserId(MAPR_ADMIN)).andStubReturn(1002);
        expect(unixUserGroupHelper.getUserId(UNKNOWN_USER)).andStubReturn(-100);
        replay(unixUserGroupHelper);
        SuppressionUtil.setAceUnixUserGroupHelper(unixUserGroupHelper);
    }

    @Test
    public void deletesMaprfsDirectory() throws IOException {
        expect(delegatedFs.delete(FOLDER_PATH)).andReturn(true);
        replayAll();

        maprfs.delete(FILE);

        verifyAll();
    }

    @Test
    public void deletesRecursivelyMaprfsDirectory() throws IOException {
        expect(delegatedFs.delete(FOLDER_PATH, true)).andReturn(true);
        replayAll();

        maprfs.deleteRecursive(FILE);

        verifyAll();
    }

    @Test
    public void createsMaprfsDirectory() throws IOException {
        expect(delegatedFs.mkdirs(FOLDER_PATH)).andReturn(true);
        replayAll();

        maprfs.mkdirs(FILE);

        verifyAll();
    }

    @Test
    public void suppressExceptionIfDirectoryWasCreated() throws IOException {
        expect(delegatedFs.mkdirs(FOLDER_PATH)).andThrow(new IOException());
        expect(delegatedFs.exists(FOLDER_PATH)).andReturn(true);
        replayAll();

        maprfs.mkdirs(FILE);

        verifyAll();
    }

    @Test
    public void rethrowExceptionIfDirectoryWasNotCreated() throws IOException {
        expect(delegatedFs.mkdirs(FOLDER_PATH)).andThrow(new IOException());
        expect(delegatedFs.exists(FOLDER_PATH)).andReturn(false);
        replayAll();

        try {
            maprfs.mkdirs(FILE);
            fail("Exception was not rethrown");
        } catch (KafkaException ignored) {
        }

        verifyAll();
    }

    @Test
    public void throwExceptionIfRequiredDirectoryDoesntExist() throws IOException {
        expect(delegatedFs.exists(FOLDER_PATH)).andReturn(false);
        replayAll();

        try {
            maprfs.requireExisting(FILE);
            fail("Exception was not thrown");
        } catch (KafkaException ignored) {
        }

        verifyAll();
    }

    @Test
    public void rethrowsExceptionAsUnchecked() throws IOException {
        expect(delegatedFs.exists(FOLDER_PATH)).andThrow(new IOException());
        replayAll();

        try {
            maprfs.exists(FILE);
            fail("Exception was not thrown");
        } catch (KafkaException ignored) {
        }

        verifyAll();
    }

    @Test
    public void returnsTrueOnAccessibleFolder() throws IOException {
        expect(KafkaMaprTools.tools().getCurrentUserName()).andReturn(MAPR_USER_1);
        MaprfsPermissions permissions = MaprfsPermissions.permissions()
                .put(MapRFileAce.AccessType.READDIR, "u:" + MAPR_USER_1)
                .put(MapRFileAce.AccessType.LOOKUPDIR, "u:" + MAPR_USER_1)
                .put(MapRFileAce.AccessType.ADDCHILD, "u:" + MAPR_USER_1)
                .put(MapRFileAce.AccessType.DELETECHILD, "u:" + MAPR_USER_1);
        expect(delegatedFs.getAces(FOLDER_PATH)).andReturn(permissions.buildAceList());
        replayAll();

        assertTrue(maprfs.isAccessibleAsDirectory(FILE));

        verifyAll();
    }

    @Test
    public void returnsTrueOnPublicFolder() throws IOException {
        expect(KafkaMaprTools.tools().getCurrentUserName()).andReturn(MAPR_USER_1);
        MaprfsPermissions permissions = MaprfsPermissions.permissions()
                .put(MapRFileAce.AccessType.READDIR, MaprfsPermissions.PUBLIC)
                .put(MapRFileAce.AccessType.LOOKUPDIR, MaprfsPermissions.PUBLIC)
                .put(MapRFileAce.AccessType.ADDCHILD, MaprfsPermissions.PUBLIC)
                .put(MapRFileAce.AccessType.DELETECHILD, MaprfsPermissions.PUBLIC);
        expect(delegatedFs.getAces(FOLDER_PATH)).andReturn(permissions.buildAceList());
        replayAll();

        assertTrue(maprfs.isAccessibleAsDirectory(FILE));

        verifyAll();
    }

    @Test
    public void returnsFalseOnMissingAces() throws IOException {
        expect(KafkaMaprTools.tools().getCurrentUserName()).andReturn(MAPR_USER_1);
        expect(delegatedFs.getAces(FOLDER_PATH)).andReturn(Collections.emptyList());
        replayAll();

        assertFalse(maprfs.isAccessibleAsDirectory(FILE));

        verifyAll();
    }

    @Test
    public void returnsFalseOnUnsatisfyingAces() throws IOException {
        expect(KafkaMaprTools.tools().getCurrentUserName()).andReturn(MAPR_USER_1);
        MaprfsPermissions permissions = MaprfsPermissions.permissions()
                .put(MapRFileAce.AccessType.READDIR, "u:" + MAPR_USER_1)
                .put(MapRFileAce.AccessType.LOOKUPDIR, "u:" + MAPR_ADMIN)
                .put(MapRFileAce.AccessType.ADDCHILD, "u:" + MAPR_USER_1)
                .put(MapRFileAce.AccessType.DELETECHILD, "u:" + MAPR_USER_1);
        expect(delegatedFs.getAces(FOLDER_PATH)).andReturn(permissions.buildAceList());
        replayAll();

        assertFalse(maprfs.isAccessibleAsDirectory(FILE));

        verifyAll();
    }

    @Test
    public void setsAcesWithList() throws IOException {
        List<MapRFileAce> permissions = MaprfsPermissions.permissions()
                .put(MapRFileAce.AccessType.READDIR, "u:" + MAPR_USER_1)
                .put(MapRFileAce.AccessType.LOOKUPDIR, "u:" + MAPR_ADMIN)
                .put(MapRFileAce.AccessType.ADDCHILD, "u:" + MAPR_USER_1)
                .put(MapRFileAce.AccessType.DELETECHILD, "u:" + MAPR_USER_1)
                .buildAceList();
        delegatedFs.setAces(FOLDER_PATH, permissions);
        expectNoinherit();
        replayAll();

        maprfs.setAces(FILE, permissions);

        verifyAll();
    }

    private void expectNoinherit() throws IOException {
        expect(delegatedFs.setAces(FOLDER_PATH, new ArrayList<>(), false, 1, 0, false, null))
                .andReturn(0);
    }

    @Test
    public void suppressIOExceptionOnSetAces() throws IOException {
        expect(KafkaMaprTools.tools().getCurrentUserName()).andReturn(MAPR_USER_1);
        List<MapRFileAce> permissions = MaprfsPermissions.permissions()
                .put(MapRFileAce.AccessType.READDIR, "u:" + MAPR_USER_1)
                .put(MapRFileAce.AccessType.LOOKUPDIR, "u:" + MAPR_ADMIN)
                .put(MapRFileAce.AccessType.ADDCHILD, "u:" + MAPR_USER_1)
                .put(MapRFileAce.AccessType.DELETECHILD, "u:" + MAPR_USER_1)
                .buildAceList();
        delegatedFs.setAces(FOLDER_PATH, permissions);
        expectLastCall().andThrow(new IOException());
        replayAll();

        maprfs.setAces(FILE, permissions);

        verifyAll();
    }

}