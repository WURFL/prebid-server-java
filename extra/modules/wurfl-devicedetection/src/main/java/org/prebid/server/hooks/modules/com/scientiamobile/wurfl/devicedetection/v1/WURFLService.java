package org.prebid.server.hooks.modules.com.scientiamobile.wurfl.devicedetection.v1;

import com.scientiamobile.wurfl.core.Device;
import com.scientiamobile.wurfl.core.WURFLEngine;
import io.vertx.core.Future;
import org.prebid.server.hooks.modules.com.scientiamobile.wurfl.devicedetection.config.WURFLDeviceDetectionConfigProperties;

import org.prebid.server.execution.file.FileProcessor;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WURFLService implements FileProcessor {

    private WURFLEngine wurflEngine;
    private WURFLDeviceDetectionConfigProperties configProperties;

    public WURFLService(WURFLEngine wurflEngine, WURFLDeviceDetectionConfigProperties configProperties) {
        this.wurflEngine = wurflEngine;
        this.configProperties = configProperties;
    }

    public Future<?> setDataPath(String dataFilePath){
        log.info("setDataPath invoked");
        return Future.succeededFuture();
    }

    public Optional<Device> lookupDevice(Map<String,String> headers) {
        return Optional.ofNullable(wurflEngine)
                .map(engine -> engine.getDeviceForRequest(headers));
    }

    public Set<String> getAllCapabilities() {
        return Optional.ofNullable(wurflEngine)
                .map(WURFLEngine::getAllCapabilities)
                .orElse(Set.of());
    }

    public Set<String> getAllVirtualCapabilities() {
        return Optional.ofNullable(wurflEngine)
                .map(WURFLEngine::getAllVirtualCapabilities)
                .orElse(Set.of());
    }
}
