package com.snoopy.registry.consul;

import com.google.common.net.HostAndPort;
import com.orbitz.consul.Consul;
import com.orbitz.consul.config.CacheConfig;
import com.orbitz.consul.config.ClientConfig;
import com.snoopy.grpc.base.configure.GrpcRegistryProperties;
import com.snoopy.grpc.base.constans.GrpcConstants;
import com.snoopy.grpc.base.registry.IRegistry;
import com.snoopy.grpc.base.registry.IRegistryProvider;
import org.springframework.util.StringUtils;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author :   kehanjiang
 * @date :   2021/12/1  15:44
 */
public class ConsulRegistryProvider implements IRegistryProvider {
    public static final String REGISTRY_PROTOCOL_CONSUL = "consul";
    public static final String PARAM_TOKEN = "token";
    public static final String PARAM_CONNECT_TIMEOUT = "connectTimeout";
    public static final String PARAM_READ_TIMEOUT = "readTimeout";
    public static final String PARAM_WRITE_TIMEOUT = "writeTimeout";
    public static final String PARAM_WATCH_DURATION = "watchDuration";

    private static final int DEFAULT_PORT = 8500;
    private static final int DEFAULT_CONNECT_TIMOUT = 5_000;
    private static final int DEFAULT_READ_TIMEOUT = 30_000;
    private static final int DEFAULT_WRITE_TIMOUT = 30_000;

    private static final int DEFAULT_WATCH_DURATION = 10;

    @Override
    public IRegistry newRegistryInstance(GrpcRegistryProperties grpcRegistryProperties) {
        Consul.Builder builder = Consul.builder();
        List<HostAndPort> hostAndPorts = Arrays.stream(GrpcConstants.ADDRESS_SPLIT_PATTERN.split(grpcRegistryProperties.getAddress()))
                .map(HostAndPort::fromString)
                .map(hostAndPort -> hostAndPort.withDefaultPort(DEFAULT_PORT))
                .collect(Collectors.toList());
        if (hostAndPorts.size() > 1) {
            String blacklistTimeInMillis = grpcRegistryProperties.getExtra(PARAM_CONNECT_TIMEOUT);
            builder.withMultipleHostAndPort(
                    hostAndPorts,
                    blacklistTimeInMillis != null ? Long.valueOf(blacklistTimeInMillis) : DEFAULT_CONNECT_TIMOUT
            );
        } else {
            builder.withHostAndPort(hostAndPorts.get(0));
        }

        String token = grpcRegistryProperties.getExtra(ConsulRegistryProvider.PARAM_TOKEN);
        if (StringUtils.hasText(token)) {
            builder.withAclToken(token);
        }

        String username = grpcRegistryProperties.getUsername();
        String password = grpcRegistryProperties.getPassword();
        if (!StringUtils.isEmpty(username) && !StringUtils.isEmpty(password)) {
            builder.withBasicAuth(username, password);
        }
        builder.withConnectTimeoutMillis(grpcRegistryProperties.getLongExtra(PARAM_CONNECT_TIMEOUT, DEFAULT_CONNECT_TIMOUT))
                .withReadTimeoutMillis(grpcRegistryProperties.getLongExtra(PARAM_READ_TIMEOUT, DEFAULT_READ_TIMEOUT))
                .withWriteTimeoutMillis(grpcRegistryProperties.getLongExtra(PARAM_WRITE_TIMEOUT, DEFAULT_WRITE_TIMOUT))
                .withClientConfiguration(new ClientConfig(CacheConfig.builder().withWatchDuration(
                        Duration.ofSeconds(grpcRegistryProperties.getLongExtra(PARAM_WATCH_DURATION, DEFAULT_WATCH_DURATION))
                ).build()));
        return new ConsulRegistry(grpcRegistryProperties, builder.build());
    }

    @Override
    public String registryType() {
        return REGISTRY_PROTOCOL_CONSUL;
    }
}
