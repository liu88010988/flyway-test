package liu;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationInfo;
import org.flywaydb.core.api.callback.BaseFlywayCallback;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

public class DatabaseInitializerBean implements InitializingBean {

    private Log log = LogFactory.getLog(getClass());
    @Autowired
    private ApplicationContext context;
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

    public void migrate() {
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
}
