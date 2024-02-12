package org.apache.kafka.connect.util;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.connect.runtime.TaskConfig;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.rest.RestServerConfig;

import java.security.PrivilegedExceptionAction;
import java.util.Map;

public class ImpersonationUtil {
    public static <T> T maybeRunImpersonated(WorkerConfig workerConfig, Map<String, String> taskProps, PrivilegedExceptionAction<T> action) {
        try {
            if (!workerConfig.getBoolean(RestServerConfig.ENABLE_IMPERSONATION_CONFIG))
                return action.run();

            String user = taskProps.get(TaskConfig.TASK_USER_CONFIG);
            if (user == null || user.isEmpty())
                user = UserGroupInformation.getCurrentUser().getShortUserName();

            UserGroupInformation ugi = UserGroupInformation.createProxyUser(user, UserGroupInformation.getCurrentUser());
            return ugi.doAs(action);
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }
}
