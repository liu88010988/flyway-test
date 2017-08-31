package liu;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationInfo;
import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.api.callback.BaseFlywayCallback;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DatabaseInitializerBean implements InitializingBean {

    private Log log = LogFactory.getLog(getClass());
    @Autowired
    private ApplicationContext applicationContext;
    @Autowired
    private Flyway flyway;
    @Autowired
    private DataSource dataSource;
    @Value("${database.scripts:false}")
    private boolean enabled;
    @Value("${database.scripts.only-patches:true}")
    private boolean onlyPatches;
    @Value("${database.scripts.name:kaola}")
    private String databaseName;
    @Value("${database.scripts.repair-on-migrate:false}")
    private boolean repairOnMigrate;

    private void migrate() {
        if (!enabled) {
            log.info("没有启用数据库自动升级");
            return;
        }
        if (dataSource == null) {
            log.warn("没有配置DataSource, 忽略数据库初始化.");
            return;
        }

        try {
            log.info("开始数据库升级");
            flyway.setCallbacks(new BaseFlywayCallback() {
                boolean baseline = false;

                @Override
                public void beforeBaseline(Connection connection) {
                    baseline = true;
                    if (!onlyPatches) {
                        if (!defaultScripts.isEmpty()) {
                            runScripts(defaultScripts, connection);
                        }
                        if (!baselineScripts.isEmpty()) {
                            runScripts(baselineScripts, connection);
                        }
                        if (!baselineDataScripts.isEmpty()) {
                            runScripts(baselineDataScripts, connection);
                        }
                    } else if (onlyPatches) {
                        log.info("忽略非补丁数据库初始化SQL脚本.");
                    } else {
                        log.info("没有数据库初始化SQL脚本.");
                    }
                }

                @Override
                public void afterMigrate(Connection connection) {
                    if (baseline && !onlyPatches && !defaultDataScripts.isEmpty()) {
                        runScripts(defaultDataScripts, connection);
                    }
                }
            });
            if (flyway.info().current() == null) {
                flyway.setBaselineVersion(baselineVersion.orElse(DEFAULT_BASELINE));
                flyway.setBaselineDescription("(BASELINE)");
                log.info(String.format("数据库基线版本: %s", flyway.getBaselineVersion()));
                flyway.baseline();
            }
            MigrationInfo[] failed = Arrays.stream(flyway.info().all())
                    .filter(info -> info.getState().isFailed())
                    .toArray(size -> new MigrationInfo[size]);
            if (failed.length > 0) {
                listMigrations(flyway);
                if (repairOnMigrate) {
                    log.warn("开始修复之前的升级错误");
                    flyway.repair();
                    log.warn("结束修复升级错误");
                } else {
                    throw new RuntimeException("Failed migration found, version=" + failed[0].getVersion());
                }
            }
            flyway.migrate();
            log.info("结束数据库升级");
        } finally {
            listMigrations(flyway);
        }
    }

    public void afterPropertiesSet() throws Exception {
        //todo 获取脚本
        initScripts();
        migrate();
    }

    private void initScripts() {
        // initBaseVersionAndScripts
        String platform = properties.getPlatform();

        if (StringUtils.hasText(baseline)
                && !VERSION_AUTO.equalsIgnoreCase(baseline)
                && !VERSION_NONE.equalsIgnoreCase(baseline)) {
            //如果指定了SQL脚本的版本
            baselineVersion = Optional.of(MigrationVersion.fromVersion(baseline));
        } else if (VERSION_AUTO.equalsIgnoreCase(baseline)) {
            //查找版本最新的脚本
            Pattern pattern = Pattern.compile(String.format("^.*%s([0-9_\\.]+)%s([\\$].*)?\\.sql$",
                    Pattern.quote(scriptPrefix),
                    String.format("(\\-%s)?", Pattern.quote(platform))));

            for (Resource resource : getResources(String.format("classpath*:%s/%s*.sql", scriptLocation, scriptPrefix))) {
                Matcher matcher = pattern.matcher(resource.getFilename());
                if (matcher.matches()) {
                    MigrationVersion version = MigrationVersion.fromVersion(matcher.group(1));
                    if (!baselineVersion.isPresent() || version.compareTo(baselineVersion.get()) > 0) {
                        baselineVersion = Optional.of(version);
                    }
                }
            }
        }
        if (StringUtils.hasText(scriptDefault)) {
            //加载默认配置脚本
            defaultScripts.addAll(getResources(String.format("classpath:%s/%s.sql", scriptLocation, scriptDefault)));
            defaultScripts.addAll(getResources(String.format("classpath*:%s/%s.*.sql", scriptLocation, scriptDefault)));
            defaultScripts.addAll(getResources(String.format("classpath*:%s/%s$*.sql", scriptLocation, scriptDefault)));

            defaultScripts.addAll(getResources(String.format("classpath:%s/%s-%s.sql", scriptLocation, scriptDefault, platform)));
            defaultScripts.addAll(getResources(String.format("classpath*:%s/%s-%s.*.sql", scriptLocation, scriptDefault, platform)));
            defaultScripts.addAll(getResources(String.format("classpath*:%s/%s-%s$*.sql", scriptLocation, scriptDefault, platform)));

            defaultDataScripts.addAll(getResources(String.format("classpath:%s/%s-data.sql", scriptLocation, scriptDefault)));
            defaultDataScripts.addAll(getResources(String.format("classpath*:%s/%s-data.*.sql", scriptLocation, scriptDefault)));
            defaultDataScripts.addAll(getResources(String.format("classpath*:%s/%s-data$*.sql", scriptLocation, scriptDefault)));
        }
        if (baselineVersion.isPresent()) {
            baselineScripts.addAll(getResources(String.format("classpath:%s/%s%s.sql", scriptLocation, scriptPrefix, versionToCode(baselineVersion.get()))));
            baselineScripts.addAll(getResources(String.format("classpath*:%s/%s%s.*.sql", scriptLocation, scriptPrefix, versionToCode(baselineVersion.get()))));
            baselineScripts.addAll(getResources(String.format("classpath*:%s/%s%s$*.sql", scriptLocation, scriptPrefix, versionToCode(baselineVersion.get()))));

            baselineScripts.addAll(getResources(String.format("classpath:%s/%s%s-%s.sql", scriptLocation, scriptPrefix, versionToCode(baselineVersion.get()), platform)));
            baselineScripts.addAll(getResources(String.format("classpath*:%s/%s%s-%s.*.sql", scriptLocation, scriptPrefix, versionToCode(baselineVersion.get()), platform)));
            baselineScripts.addAll(getResources(String.format("classpath*:%s/%s%s-%s$*.sql", scriptLocation, scriptPrefix, versionToCode(baselineVersion.get()), platform)));

            baselineDataScripts.addAll(getResources(String.format("classpath:%s/%s%s-data.sql", scriptLocation, scriptPrefix, versionToCode(baselineVersion.get()))));
            baselineDataScripts.addAll(getResources(String.format("classpath*:%s/%s%s-data.*.sql", scriptLocation, scriptPrefix, versionToCode(baselineVersion.get()))));
            baselineDataScripts.addAll(getResources(String.format("classpath*:%s/%s%s-data$*.sql", scriptLocation, scriptPrefix, versionToCode(baselineVersion.get()))));
        }
    }

    private void runScripts(Collection<Resource> resources, Connection connection) {
        ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
        populator.setContinueOnError(false);
        populator.setSqlScriptEncoding(StandardCharsets.UTF_8.name());
        for (Resource resource : resources) {
            log.info(String.format("准备SQL脚本: %s", resource));
            populator.addScript(resource);
        }
        populator.populate(connection);
    }

    private void listMigrations(Flyway flyway) {
        log.info(String.format("数据库 - %s: \n%s", databaseName, Arrays.stream(flyway.info().all())
                .map(info -> String.format("\t%s - %s - %s - %s - %s\n",
                        info.getVersion(),
                        info.getState(),
                        info.getDescription(),
                        info.getScript(),
                        info.getInstalledOn()))
                .collect(Collectors.joining())));
    }

    private List<Resource> getResources(String locations) {
        List<Resource> resources = new ArrayList<Resource>();
        for (String location : StringUtils.commaDelimitedListToStringArray(locations)) {
            try {
                for (Resource resource : this.applicationContext.getResources(location)) {
                    if (resource.exists()) {
                        resources.add(resource);
                    }
                }
            } catch (IOException ex) {
                throw new IllegalStateException(
                        "Unable to load resource from " + location, ex);
            }
        }
        return resources;
    }
}
