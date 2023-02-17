package org.apache.kafka.mapr.tools;

import com.mapr.baseutils.utils.AceHelper;
import com.mapr.fs.ShimLoader;
import com.mapr.security.UnixUserGroupHelper;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hadoop.conf.Configuration;

import java.util.List;

public class SuppressionUtil {
    public static void clearDefaultResources() throws IllegalAccessException {
        ((List<?>) FieldUtils.readStaticField(Configuration.class, "defaultResources", true)).clear();
    }

    public static void setAceUnixUserGroupHelper(UnixUserGroupHelper unixUserGroupHelper) throws IllegalAccessException {
        FieldUtils.writeStaticField(AceHelper.class, "ugHelper", unixUserGroupHelper, true);
    }

    public static void suppressShimLoader() throws IllegalAccessException {
        FieldUtils.writeStaticField(ShimLoader.class, "isLoaded", true, true);
    }
}
