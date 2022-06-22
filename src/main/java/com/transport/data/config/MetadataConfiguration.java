package com.transport.data.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.springframework.boot.autoconfigure.batch.BatchDataSource;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.util.StringUtils;

@Configuration
public class MetadataConfiguration {
    @Bean
    @BatchDataSource
    public HikariDataSource metaDatasource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl( "jdbc:h2:./h2db/meta" );
        config.setUsername("sa");
        config.setDriverClassName("org.h2.Driver");
        return new HikariDataSource( config );
    }

    @Bean
    @Primary
    public HikariDataSource dataSource(DataSourceProperties properties) {
        HikariDataSource dataSource = properties.initializeDataSourceBuilder().type(HikariDataSource.class).build();
        if (StringUtils.hasText(properties.getName())) {
            dataSource.setPoolName(properties.getName());
        }
        return dataSource;
    }
}