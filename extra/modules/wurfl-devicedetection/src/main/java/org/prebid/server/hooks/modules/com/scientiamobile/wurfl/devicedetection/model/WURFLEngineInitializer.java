package org.prebid.server.hooks.modules.com.scientiamobile.wurfl.devicedetection.model;

import com.scientiamobile.wurfl.core.GeneralWURFLEngine;
import com.scientiamobile.wurfl.core.WURFLEngine;
import com.scientiamobile.wurfl.core.cache.LRUMapCacheProvider;
import com.scientiamobile.wurfl.core.cache.NullCacheProvider;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.prebid.server.hooks.modules.com.scientiamobile.wurfl.devicedetection.config.WURFLDeviceDetectionConfigProperties;
import org.prebid.server.hooks.modules.com.scientiamobile.wurfl.devicedetection.exc.WURFLModuleConfigurationException;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Builder
public class WURFLEngineInitializer {

    private static final Set<String> REQUIRED_STATIC_CAPS = Set.of(
            "ajax_support_javascript",
            "brand_name",
            "density_class",
            "is_connected_tv",
            "is_ott",
            "is_tablet",
            "model_name",
            "resolution_height",
            "resolution_width",
            "physical_form_factor");

    private WURFLDeviceDetectionConfigProperties configProperties;

    private String wurflFilePath;

    public WURFLEngine initWURFLEngine() {
        // this happens in a spring bean that happens at module startup time
        return initializeEngine(configProperties, wurflFilePath);
    }

    static WURFLEngine initializeEngine(WURFLDeviceDetectionConfigProperties configProperties, String wurflInFilePath) {

        String wurflFilePath = wurflInFilePath;
        if (wurflFilePath == null) {
            final String wurflFileName = extractWURFLFileName(configProperties.getFileSnapshotUrl());

            final Path wurflPath = Paths.get(
                    configProperties.getFileDirPath(),
                    wurflFileName);
            wurflFilePath = wurflPath.toAbsolutePath().toString();
        }
        final WURFLEngine engine = new GeneralWURFLEngine(wurflFilePath);
        verifyStaticCapabilitiesDefinition(engine);

        if (configProperties.getCacheSize() > 0) {
            engine.setCacheProvider(new LRUMapCacheProvider(configProperties.getCacheSize()));
        } else {
            engine.setCacheProvider(new NullCacheProvider());
        }
        return engine;
    }

    public static String extractWURFLFileName(String wurflSnapshotUrl) {

        try {
            final URI uri = new URI(wurflSnapshotUrl);
            final String path = uri.getPath();
            return path.substring(path.lastIndexOf('/') + 1);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid WURFL snapshot URL: " + wurflSnapshotUrl, e);
        }
    }

    static void verifyStaticCapabilitiesDefinition(WURFLEngine engine) {

        final List<String> unsupportedStaticCaps = new ArrayList<>();
        final Map<String, Boolean> allCaps = engine.getAllCapabilities().stream()
                .collect(Collectors.toMap(
                        key -> key,
                        value -> true
                ));

        for (String requiredCapName : REQUIRED_STATIC_CAPS) {
            if (!allCaps.containsKey(requiredCapName)) {
                unsupportedStaticCaps.add(requiredCapName);
            }
        }

        if (!unsupportedStaticCaps.isEmpty()) {
            Collections.sort(unsupportedStaticCaps);
            final String failedCheckMessage = """
                                Static capabilities  %s needed for device enrichment are not defined in WURFL.
                                Please make sure that your license has the needed capabilities or upgrade it.
                    """.formatted(String.join(",", unsupportedStaticCaps));

            throw new WURFLModuleConfigurationException(failedCheckMessage);
        }

    }
}
