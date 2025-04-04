package org.prebid.server.hooks.modules.com.scientiamobile.wurfl.devicedetection.config;

import com.scientiamobile.wurfl.core.WURFLEngine;
import org.prebid.server.hooks.modules.com.scientiamobile.wurfl.devicedetection.model.WURFLEngineInitializer;
import org.prebid.server.hooks.modules.com.scientiamobile.wurfl.devicedetection.v1.WURFLDeviceDetectionEntrypointHook;
import org.prebid.server.hooks.modules.com.scientiamobile.wurfl.devicedetection.v1.WURFLDeviceDetectionModule;
import org.prebid.server.hooks.modules.com.scientiamobile.wurfl.devicedetection.v1.WURFLDeviceDetectionRawAuctionRequestHook;
import org.prebid.server.hooks.modules.com.scientiamobile.wurfl.devicedetection.v1.WURFLService;
import org.prebid.server.spring.env.YamlPropertySourceFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import io.vertx.core.Vertx;
import org.prebid.server.execution.file.syncer.FileSyncer;
import org.prebid.server.spring.config.model.FileSyncerProperties;
import org.prebid.server.spring.config.model.HttpClientProperties;
import org.prebid.server.execution.file.FileUtil;

import java.nio.file.Path;
import java.util.List;

@ConditionalOnProperty(prefix = "hooks." + WURFLDeviceDetectionModule.CODE, name = "enabled", havingValue = "true")
@Configuration
@PropertySource(
        value = "classpath:/module-config/wurfl-devicedetection.yaml",
        factory = YamlPropertySourceFactory.class)
@EnableConfigurationProperties(WURFLDeviceDetectionConfigProperties.class)
public class WURFLDeviceDetectionConfiguration {

    private static final Long DAILY_SYNC_INTERVAL = 86400000L;

    @Bean
    public WURFLDeviceDetectionModule wurflDeviceDetectionModule(WURFLDeviceDetectionConfigProperties
                                                                         configProperties, Vertx vertx) {

        final WURFLEngine wurflEngine = WURFLEngineInitializer.builder()
                .configProperties(configProperties)
                .build().initWURFLEngine();
        wurflEngine.load();

        final WURFLService wurflService = new WURFLService(wurflEngine, configProperties);

        if (configProperties.isWurflRunUpdater()) {
            final FileSyncer fileSyncer = createFileSyncer(configProperties, wurflService, vertx);
            fileSyncer.sync();
        }

        return new WURFLDeviceDetectionModule(List.of(new WURFLDeviceDetectionEntrypointHook(),
                new WURFLDeviceDetectionRawAuctionRequestHook(wurflService, configProperties)));
    }

    private FileSyncer createFileSyncer(WURFLDeviceDetectionConfigProperties configProperties,
                                        WURFLService wurflService, Vertx vertx) {

        final String snapshotURL = configProperties.getWurflSnapshotUrl();
        String downloadPath = configProperties.getWurflFileDirPath();
        String tempPath = downloadPath;
        if (snapshotURL.endsWith(".xml.gz")) {
            downloadPath = Path.of(downloadPath, "new_wurfl.xml.gz").toString();
            tempPath = Path.of(tempPath, "temp_wurfl.xml.gz").toString();
        } else {
            downloadPath = Path.of(downloadPath, "new_wurfl.zip").toString();
            tempPath = Path.of(tempPath, "temp_wurfl.zip").toString();
        }

        final HttpClientProperties httpProperties = new HttpClientProperties();
        httpProperties.setConnectTimeoutMs(configProperties.getUpdateConnTimeoutMs());
        httpProperties.setMaxRedirects(1);

        final FileSyncerProperties fileSyncerProperties = new FileSyncerProperties();
        fileSyncerProperties.setCheckSize(true);
        fileSyncerProperties.setDownloadUrl(configProperties.getWurflSnapshotUrl());
        fileSyncerProperties.setSaveFilepath(downloadPath);
        fileSyncerProperties.setTmpFilepath(tempPath);
        fileSyncerProperties.setTimeoutMs((long) configProperties.getUpdateConnTimeoutMs());
        fileSyncerProperties.setUpdateIntervalMs(DAILY_SYNC_INTERVAL);
        fileSyncerProperties.setRetryCount(configProperties.getUpdateRetries());
        fileSyncerProperties.setRetryIntervalMs(configProperties.getRetryIntervalMs());
        fileSyncerProperties.setHttpClient(httpProperties);

        return FileUtil.fileSyncerFor(
                wurflService,
                fileSyncerProperties,
                vertx);
    }

}
