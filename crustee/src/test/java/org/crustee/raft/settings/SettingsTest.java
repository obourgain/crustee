package org.crustee.raft.settings;

import static org.assertj.core.api.Assertions.assertThat;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.assertj.core.api.ThrowableAssert;
import org.junit.BeforeClass;
import org.junit.Test;

public class SettingsTest {

    static Path resourcesRoot;

    @BeforeClass
    public static void ensure_path() {
        resourcesRoot = Paths.get("target/test-classes");
        assertThat(resourcesRoot).exists();

        Path settings = resourcesRoot.resolve("test-settings.yml");
        assertThat(settings).exists();
    }

    @Test
    public void should_load_settings() throws Exception {
        Settings settings = new Settings(settingsFile("test-settings.yml"));
        assertThat(settings.getRingbufferSize()).isEqualTo(2048);
        assertThat(settings.getCommitLogSegmentSize()).isEqualTo(10);
    }

    @Test
    public void should_fail_to_load_non_existing_settings() throws Exception {
        Throwable throwable = ThrowableAssert.catchThrowable(() -> new Settings(settingsFile("dont-exists.yml")));
        assertThat(throwable).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void should_fail_to_load_empty_settings() throws Exception {
        Throwable throwable = ThrowableAssert.catchThrowable(() -> new Settings(settingsFile("empty-test-settings.yml")));
        assertThat(throwable).isInstanceOf(IllegalStateException.class);
    }

    private Path settingsFile(String file) throws URISyntaxException {
        return resourcesRoot.resolve(file);
    }

}
