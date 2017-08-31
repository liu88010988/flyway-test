package liu;

import org.flywaydb.core.Flyway;
import org.h2.jdbcx.JdbcDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.Resource;

import javax.sql.DataSource;
import java.sql.SQLException;

@Configuration
public class H2Configuration {
    private static final String H2_JDBC_URL = "jdbc:h2:mem:test;MODE=MySQL;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE;DATABASE_TO_UPPER=false";
    @Value("classpath:mybatis/*.xml")
    private Resource[] resource;

    @Bean
    @Primary
    @Profile("test")
    public DataSource dataSource() throws SQLException {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL(H2_JDBC_URL);
        dataSource.setUser("sa");
        dataSource.setPassword("");
        return dataSource;
    }


    @Bean
    @Profile("test")
    public Flyway flyway(DataSource dataSource) {
        Flyway flyway = new Flyway();
        flyway.setDataSource(dataSource);
        return flyway;
    }

    @Bean
    @Profile("test")
    public DatabaseInitializerBean databaseInitializerBean() {
        return new DatabaseInitializerBean();
    }
}
