package org.apache.kafka.mapr.tools;

import com.mapr.fs.MapRFileAce;
import com.mapr.security.UnixUserGroupHelper;
import org.junit.Test;

import java.util.List;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;

public class MaprfsPermissionsTest {

    private static final String MAPRUSER_1 = "mapruser1";
    private static final String MAPRUSER_1_ACE = "u:" + MAPRUSER_1;
    private static final String MAPR = "mapr";
    private static final String MAPR_ACE = "u:" + MAPR;

    @Test
    public void parseEmptyPermissionsFromEmptyConfigurationValue() {
        MaprfsPermissions permissions = MaprfsPermissions.permissions().loadFromConfig("");
        assertEquals(permissions, MaprfsPermissions.permissions());
    }

    @Test
    public void parseFewPermissionsFromConfigurationValue() {
        MaprfsPermissions permissions = MaprfsPermissions.permissions()
                .loadFromConfig("readdir=u:mapr; lookupdir=u:mapruser1");
        assertEquals(permissions, MaprfsPermissions.permissions()
                .put(MapRFileAce.AccessType.READDIR, MAPR_ACE)
                .put(MapRFileAce.AccessType.LOOKUPDIR, MAPRUSER_1_ACE));
    }

    @Test
    public void parsePermissionsFromConfigurationValueWithDuplicates() {
        MaprfsPermissions permissions = MaprfsPermissions.permissions()
                .loadFromConfig("readdir=u:mapr; lookupdir=u:mapruser1; readdir=u:mapruser1");
        assertEquals(permissions, MaprfsPermissions.permissions()
                .put(MapRFileAce.AccessType.READDIR, MAPRUSER_1_ACE)
                .put(MapRFileAce.AccessType.LOOKUPDIR, MAPRUSER_1_ACE));
    }

    @Test
    public void parsePermissionsFromConfigurationValueWithSubstitution() {
        KafkaMaprTools.tools = mock(KafkaMaprTools.class);
        expect(KafkaMaprTools.tools().getCurrentUserName()).andStubReturn(MAPRUSER_1);
        expect(KafkaMaprTools.tools().getClusterAdminUserName()).andStubReturn(MAPR);
        replay(KafkaMaprTools.tools());

        MaprfsPermissions permissions = MaprfsPermissions.permissions()
                .loadFromConfig("readdir=<cluster.admin> | <startup.user>");
        assertEquals(permissions, MaprfsPermissions.permissions()
                .put(MapRFileAce.AccessType.READDIR, "u:mapr | u:mapruser1"));
    }

    @Test
    public void parsePermissionsFromConfigurationValueAndOverrideOldValues() {
        MaprfsPermissions permissions = MaprfsPermissions.permissions()
                .put(MapRFileAce.AccessType.READDIR, MAPR_ACE)
                .put(MapRFileAce.AccessType.LOOKUPDIR, MAPR_ACE)
                .put(MapRFileAce.AccessType.ADDCHILD, MAPR_ACE)
                .put(MapRFileAce.AccessType.DELETECHILD, MAPR_ACE)
                .loadFromConfig("readdir=u:mapr; lookupdir=u:mapruser1; readdir=u:mapruser1");

        assertEquals(permissions, MaprfsPermissions.permissions()
                .put(MapRFileAce.AccessType.READDIR, MAPRUSER_1_ACE)
                .put(MapRFileAce.AccessType.LOOKUPDIR, MAPRUSER_1_ACE)
                .put(MapRFileAce.AccessType.ADDCHILD, MAPR_ACE)
                .put(MapRFileAce.AccessType.DELETECHILD, MAPR_ACE));
    }

    @Test
    public void buildAceList() throws IllegalAccessException {
        UnixUserGroupHelper idMapper = mock(UnixUserGroupHelper.class);
        SuppressionUtil.setAceUnixUserGroupHelper(idMapper);
        expect(idMapper.getUserId(MAPRUSER_1)).andStubReturn(1001);
        expect(idMapper.getUserId(MAPR)).andStubReturn(1000);
        replay(idMapper);

        List<MapRFileAce> aceList = MaprfsPermissions.permissions()
                .put(MapRFileAce.AccessType.READDIR, MAPRUSER_1_ACE)
                .put(MapRFileAce.AccessType.LOOKUPDIR, MAPRUSER_1_ACE)
                .put(MapRFileAce.AccessType.ADDCHILD, MAPR_ACE)
                .put(MapRFileAce.AccessType.DELETECHILD, MAPR_ACE)
                .buildAceList();

        assertEquals(4, aceList.size());
        assertThat(findFromList(aceList, MapRFileAce.AccessType.READDIR), equalTo(MAPRUSER_1_ACE));
        assertThat(findFromList(aceList, MapRFileAce.AccessType.LOOKUPDIR), equalTo(MAPRUSER_1_ACE));
        assertThat(findFromList(aceList, MapRFileAce.AccessType.ADDCHILD), equalTo(MAPR_ACE));
        assertThat(findFromList(aceList, MapRFileAce.AccessType.DELETECHILD), equalTo(MAPR_ACE));
    }

    private String findFromList(List<MapRFileAce> aceList, MapRFileAce.AccessType type) {
        return aceList.stream()
                .filter(ace -> ace.getAccessType() == type)
                .map(MapRFileAce::getBooleanExpression)
                .findFirst()
                .orElse(null);
    }
}