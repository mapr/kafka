package org.apache.kafka.mapr.tools;

import com.mapr.fs.MapRFileAce;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.common.KafkaException;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.split;
import static org.apache.commons.lang3.StringUtils.substringAfter;
import static org.apache.commons.lang3.StringUtils.substringBefore;

/**
 * Sample with .properties file value in format (order and case insensitive for keys):
 * <prefix>.permissions=readdir=p; lookupdir=p; addchild=u:<startup.user>; removechild=u:<startup.user>
 */
public class MaprfsPermissions {
    /**
     * ${@link UserGroupInformation#getCurrentUser()} at startup time
     */
    public static final String STARTUP_USER = "<startup.user>";

    /**
     * ${@link UserGroupInformation#getLoginUser()} at startup time
     */
    public static final String CLUSTER_ADMIN = "<cluster.admin>";
    public static final String PUBLIC = "p";

    private final Map<MapRFileAce.AccessType, String> aces;

    private MaprfsPermissions() {
        aces = new HashMap<>();
    }

    public static MaprfsPermissions permissions() {
        return new MaprfsPermissions();
    }

    public MaprfsPermissions loadFromConfig(String configurationValue) {
        for (String entry : split(configurationValue.trim(), ';')) {
            String trimmed = entry.trim();
            put(parseAceKey(trimmed), parseAceValue(trimmed));
        }
        return this;
    }

    public MaprfsPermissions put(MapRFileAce.AccessType permission, String value) {
        aces.put(permission, substituteExpression(value));
        return this;
    }

    public List<MapRFileAce> buildAceList() {
        return aces.entrySet().stream()
                .map(entry -> createFileAce(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
    }

    private static MapRFileAce.AccessType parseAceKey(String line) {
        String key = substringBefore(line, "=");
        return MapRFileAce.AccessType.valueOf(key.toUpperCase());
    }

    private static String parseAceValue(String line) {
        return substringAfter(line, "=");
    }

    private static String substituteExpression(String expression) {
        if (expression.contains(STARTUP_USER)) {
            expression = expression.replaceAll(STARTUP_USER, "u:" + KafkaMaprTools.tools().getCurrentUserName());
        }
        if (expression.contains(CLUSTER_ADMIN)) {
            expression = expression.replaceAll(CLUSTER_ADMIN, "u:" + KafkaMaprTools.tools().getClusterAdminUserName());
        }
        return expression;
    }

    private static MapRFileAce createFileAce(MapRFileAce.AccessType type, String expression) {
        try {
            MapRFileAce ace = new MapRFileAce(type);
            ace.setBooleanExpression(expression);
            return ace;
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MaprfsPermissions that = (MaprfsPermissions) o;
        return Objects.equals(aces, that.aces);
    }

    @Override
    public int hashCode() {
        return Objects.hash(aces);
    }
}
