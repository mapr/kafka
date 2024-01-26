package org.apache.kafka.mapr.tools;

import com.mapr.baseutils.utils.AceHelper;
import com.mapr.fs.MapRFileAce;
import com.mapr.fs.MapRFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaMaprfs implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaMaprfs.class);
    private static final int NOINHERIT = 1;
    private final MapRFileSystem fs;
    private final Map<String, Path> paths = new HashMap<>();

    public KafkaMaprfs(MapRFileSystem fs) {
        this.fs = fs;
    }

    private Path at(String path) {
        return paths.computeIfAbsent(path, Path::new);
    }

    public boolean isAccessibleAsDirectory(String path) {
        return isAccessibleWith(path,
                                MapRFileAce.AccessType.READDIR,
                                MapRFileAce.AccessType.ADDCHILD,
                                MapRFileAce.AccessType.LOOKUPDIR,
                                MapRFileAce.AccessType.DELETECHILD
        );
    }

    public boolean isAccessibleWith(String path, MapRFileAce.AccessType... permissions) {
        String postfix = toPostfix("u:" + KafkaMaprTools.tools().getCurrentUserName());
        List<MapRFileAce> aces = this.getAces(path);
        return Arrays.stream(permissions).allMatch(accessType -> aces.stream()
                .filter(mapRFileAce -> mapRFileAce.getAccessType() == accessType)
                .map(MapRFileAce::getBooleanExpression)
                .anyMatch(expression -> hasGrantedAccessAs(expression, postfix)));
    }

    public List<MapRFileAce> getAces(String path) {
        try {
            return fs.getAces(at(path));
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }

    private static String toPostfix(String expression) {
        try {
            return AceHelper.toPostfix(expression);
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }

    private static boolean hasGrantedAccessAs(String expression, String userPostfix) {
        return Arrays.stream(expression.split(","))
                .map(KafkaMaprfs::toPostfix)
                .anyMatch(postfix -> postfix.equals(userPostfix) || postfix.equals("p"));
    }

    public void setAces(String path, List<MapRFileAce> aces) {
        try {
            // Set other aces
            fs.setAces(at(path), aces);

            // Setting inherits to false
            fs.setAces(at(path), new ArrayList<>(), false,
                       NOINHERIT, 0, false, null);
        } catch (IOException e) {
            String user = KafkaMaprTools.tools().getCurrentUserName();
            LOG.warn("Failed to change permissions on file '{}' as '{}'", path, user, e);
        }
    }

    public void setPermissions(String path, MaprfsPermissions permissions) {
        setAces(path, permissions.buildAceList());
    }

    public void mkdirs(String path) {
        try {
            fs.mkdirs(at(path));
        } catch (IOException e) {
            if (!exists(path)) {
                throw new KafkaException(e);
            }
        }
    }

    public boolean exists(String path) {
        try {
            return fs.exists(at(path));
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }

    public void requireExisting(String path) {
        if (!exists(path)) {
            throw new KafkaException(String.format("File '%s' doesn't exist", path));
        }
    }

    public void requireParentExisting(String path) {
        requireExisting(at(path).getParent().toUri().getRawPath());
    }

    public void delete(String path) {
        try {
            fs.delete(at(path));
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }

    public void deleteRecursive(String path) {
        try {
            fs.delete(at(path), true);
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }

    public MapRFileSystem getFileSystem() {
        return fs;
    }

    @Override
    public void close() {
        try {
            if (fs != null)
                fs.close();
        } catch (IOException e) {
            throw new KafkaException(e);
        }
    }
}
