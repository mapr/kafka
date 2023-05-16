/* Copyright (c) 2023 & onwards. Hewlett Packard Enterprise Company, All rights reserved */
package com.mapr.kafka.eventstreams.kwps;

import static com.mapr.kafka.eventstreams.kwps.KWPSCommon.errMsg;
import static com.mapr.kafka.eventstreams.kwps.KWPSCommon.getVolumeName;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.TopicExistsException;

import com.mapr.baseutils.cldbutils.CLDBRpcCommonUtils;
import com.mapr.fs.MapRFileSystem;
import com.mapr.fs.cldb.proto.CLDBProto;
import com.mapr.fs.cldb.proto.CLDBProto.VolumeCreateRequest;
import com.mapr.fs.cldb.proto.CLDBProto.VolumeCreateResponse;
import com.mapr.fs.cldb.proto.CLDBProto.VolumeLookupRequest;
import com.mapr.fs.cldb.proto.CLDBProto.VolumeLookupResponse;
import com.mapr.fs.cldb.proto.CLDBProto.VolumeProperties;
import com.mapr.fs.cldb.proto.CLDBProto.VolumeRemoveRequest;
import com.mapr.fs.cldb.proto.CLDBProto.VolumeRemoveResponse;
import com.mapr.fs.jni.Errno;
import com.mapr.fs.jni.MapRUserInfo;
import com.mapr.fs.proto.Common;
import com.mapr.fs.proto.Common.FidMsg;
import com.mapr.fs.proto.Security.CredentialsMsg;
import com.mapr.fs.proto.Security.CredentialsMsg.Builder;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class VolumeManager {
  private static final FsPermission TOPIC_VOLUME_PERMISSION = new FsPermission(0755);

  private static final int MAX_RETRIES = 5;

  private final MapRFileSystem mfs;
  private final MapRUserInfo userInfo;
  private final CredentialsMsg creds;

  public VolumeManager(final MapRFileSystem mfs, final CredentialsMsg userCredentials) {
    this.mfs = mfs;
    this.userInfo = mfs.getUserInfo();

    if (userCredentials == null) {
      final Builder msg = CredentialsMsg.newBuilder()
          .setUid(userInfo.GetUserID())
          .setUserName(userInfo.getUserName());
      for (int gid : userInfo.GetGroupIDList()) {
        msg.addGids(gid);
      }
      this.creds = msg.build();
    } else {
      this.creds = userCredentials;
    }
  }

  public void createVolume(final String volumeName, String volumePath) throws IllegalArgumentException, IOException {
    int uid = userInfo.GetUserID();
    int gid = userInfo.GetGroupIDList()[0];
    final VolumeProperties volProps = VolumeProperties.newBuilder()
        .setVolumeName(volumeName)
        .setOwnerId(uid)
        .setRootDirUser(uid)
        .setRootDirGroup(gid)
        .build();

    final VolumeCreateRequest volumeCreateReq = VolumeCreateRequest.newBuilder()
        .setCreds(creds)
        .setVolProperties(volProps)
        .build();

    try {
      final VolumeCreateResponse resp = createVolume(volumeCreateReq);
      int status = resp.getStatus();
      if (status == 0) {
        String cluster = null; // local cluster
        int mountStatus = mfs.mountVolume(cluster, volumeName, volumePath, userInfo.getUserName());
        if (mountStatus != 0) {
          throw new IOException(
              errMsg(mountStatus, "Failed to mount volume '%s' at path '%s'", volumeName, volumePath));
        }

        setPermissionWithRetry(volumePath);
        log.debug("Created volume: '{}' mounted at path '{}'.", volumeName, volumePath);
      } else {
        String errMsg = errMsg(status, "Failed to create volume '%s' at path '%s'.", volumeName, volumePath);
        switch (status) {
        case Errno.EPERM:
        case Errno.EACCES:
          throw new TopicAuthorizationException(errMsg(status, "User does not have permission to create volumes for topics."));
        case Errno.EEXIST:
          throw new TopicExistsException(errMsg);
        default:
          throw new IOException(errMsg);
        }
      }
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public boolean isVolume(final String topicName) throws IllegalArgumentException, IOException {
    try {
      final String volumeName = getVolumeName(topicName);
      final VolumeLookupRequest lookupRequest = VolumeLookupRequest.newBuilder()
          .setVolumeName(volumeName).setCreds(creds).build();
      final VolumeLookupResponse lookupResp = lookupVolume(lookupRequest);
      return lookupResp.getStatus() == 0;
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public boolean deleteVolume(final String topicName) throws IllegalArgumentException, IOException {
    try {
      final String volumeName = getVolumeName(topicName);

      // Lookup
      final VolumeLookupRequest lookupRequest = VolumeLookupRequest.newBuilder()
          .setVolumeName(volumeName).setCreds(creds).build();
      final VolumeLookupResponse lookupResp = lookupVolume(lookupRequest);
      int status = lookupResp.getStatus();
      if (status != 0) {
        throw new IOException(errMsg(status, "Error looking up topic volume '%s'.", volumeName));
      }

      final VolumeProperties volProps = lookupResp.getVolInfo().getVolProperties();
      if (volProps.getMounted()) {
        final String volumeMountDir = volProps.getMountDir();
        final FidMsg parentFid = volProps.getParentFid();
        status = mfs.unmountVolume(null, volumeName, volumeMountDir, userInfo.getUserName(),
            parentFid.getCid(), parentFid.getCinum(), parentFid.getUniq());
        if (status != 0) {
          throw new IOException(errMsg(status, "Failed to unmount topic volume '%s'.", volumeName));
        }
      }

      // remove
      final VolumeRemoveRequest volumeRemove =  VolumeRemoveRequest.newBuilder()
          .setVolumeName(volumeName)
          .setCreds(creds)
          .setForceRemove(true)
          .build();
      final VolumeRemoveResponse removeResp = removeVolume(volumeRemove);
      if (removeResp.getStatus() != 0) {
        throw new IOException(errMsg(removeResp.getStatus(), "Failed to delete topic volume '%s'.", volumeName));
      }
      return true;
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private VolumeRemoveResponse removeVolume(VolumeRemoveRequest volumeRemoveReq) throws Exception {
    final byte[] data = CLDBRpcCommonUtils.getInstance()
        .sendRequest(
            Common.MapRProgramId.CldbProgramId.getNumber(),
            CLDBProto.CLDBProg.VolumeRemoveProc.getNumber(),
            volumeRemoveReq, VolumeRemoveResponse.class);
    return (data == null) ? null : VolumeRemoveResponse.parseFrom(data);
  }

  private VolumeCreateResponse createVolume(VolumeCreateRequest volumeCreateReq) throws Exception {
    final byte[] data = CLDBRpcCommonUtils.getInstance()
        .sendRequest(
            Common.MapRProgramId.CldbProgramId.getNumber(),
            CLDBProto.CLDBProg.VolumeCreateProc.getNumber(),
            volumeCreateReq, VolumeCreateResponse.class);
    return (data == null) ? null : VolumeCreateResponse.parseFrom(data);
  }

  private VolumeLookupResponse lookupVolume(VolumeLookupRequest volumeLookupReq) throws Exception {
    final byte[] data = CLDBRpcCommonUtils.getInstance()
        .sendRequest(
            Common.MapRProgramId.CldbProgramId.getNumber(),
            CLDBProto.CLDBProg.VolumeLookupProc.getNumber(),
            volumeLookupReq, VolumeLookupResponse.class);
    return (data == null) ? null : VolumeLookupResponse.parseFrom(data);
  }

  private boolean setPermissionWithRetry(String volumePathStr) throws IOException {
    int retry = 0;
    int sleep = 100;
    int totalSleep = 0;
    while (true) {
      try {
        final Path volumePath = new Path(volumePathStr);
        mfs.setPermission(volumePath, TOPIC_VOLUME_PERMISSION);
        return true;
      } catch (IOException e) {
        if (retry++ < MAX_RETRIES) {
          log.debug("Volume is not ready yet, retrying ({}) in {}ms", retry, sleep);
          try {
            Thread.sleep(sleep);
            totalSleep += sleep;
            sleep *= 2;
          } catch (InterruptedException e1) {}
          continue;
        }
        throw new IOException("Topic volume isn't online after " + totalSleep + "ms.", e);
      }
    }
  }

}
