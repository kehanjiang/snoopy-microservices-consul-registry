package com.snoopy.registry.consul;

import com.google.common.hash.Hashing;
import com.orbitz.consul.Consul;
import com.orbitz.consul.HealthClient;
import com.orbitz.consul.cache.ServiceHealthCache;
import com.orbitz.consul.cache.ServiceHealthKey;
import com.orbitz.consul.model.agent.ImmutableRegistration;
import com.orbitz.consul.model.agent.Registration;
import com.orbitz.consul.model.health.Service;
import com.orbitz.consul.model.health.ServiceHealth;
import com.snoopy.grpc.base.configure.GrpcRegistryProperties;
import com.snoopy.grpc.base.constans.GrpcConstants;
import com.snoopy.grpc.base.registry.IRegistry;
import com.snoopy.grpc.base.registry.ISubscribeCallback;
import com.snoopy.grpc.base.registry.RegistryServiceInfo;
import com.snoopy.grpc.base.utils.LoggerBaseUtil;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * @author :   kehanjiang
 * @date :   2021/12/1  15:18
 */
public class ConsulRegistry implements IRegistry {
    public static final String PARAM_TTL = "ttl";
    private static final long DEFAULT_TTL = 10L;
    private static final String PARAM_TAGS = "tags";
    private static final String TAG_SERVICE = "snoopy";

    private Consul client;
    private ServiceHealthCache svHealth;
    private long ttl;
    private ScheduledExecutorService ttlConsulCheckExecutor;

    private Map<String, RegistryServiceInfo> ttlCheckIds = new ConcurrentHashMap<>();
    private final ReentrantLock reentrantLock = new ReentrantLock();

    public ConsulRegistry(GrpcRegistryProperties grpcRegistryProperties, Consul client) {
        this.client = client;
        ttl = grpcRegistryProperties.getLongExtra(PARAM_TTL, DEFAULT_TTL);
        ttlConsulCheckExecutor = new ScheduledThreadPoolExecutor(1,
                new NamedThreadFactory("Consul-ttl-Executor"));
        ttlConsulCheckExecutor.scheduleAtFixedRate(this::checkPass, ttl * 1000 / 4,
                ttl * 1000 / 4, TimeUnit.MILLISECONDS);
    }


    @Override
    public void subscribe(RegistryServiceInfo serviceInfo, ISubscribeCallback subscribeCallback) {
        reentrantLock.lock();
        try {
            HealthClient healthClient = client.healthClient();
            String serviceName = buildName(serviceInfo);
            svHealth = ServiceHealthCache.newCache(healthClient, serviceName);
            svHealth.addListener((Map<ServiceHealthKey, ServiceHealth> newValues) -> {
                // do something with updated server map
                Collection<ServiceHealth> serviceHealths = newValues.values();
                List<RegistryServiceInfo> serviceInfoList = serviceHealths != null ? serviceHealths.stream().map(serviceHealth -> {
                    Service service = serviceHealth.getService();
                    return new RegistryServiceInfo(
                            serviceInfo.getNamespace(),
                            service.getService(),
                            ConsulRegistryProvider.REGISTRY_PROTOCOL_CONSUL,
                            service.getAddress(),
                            service.getPort(),
                            service.getMeta());
                }).collect(Collectors.toList()) : Collections.EMPTY_LIST;
                subscribeCallback.handle(serviceInfoList);
            });
            svHealth.start();
        } catch (Throwable e) {
            throw new RuntimeException("[" + serviceInfo.getPath() + "] subscribe failed !", e);
        } finally {
            reentrantLock.unlock();
        }
    }

    @Override
    public void unsubscribe(RegistryServiceInfo serviceInfo) {
        reentrantLock.lock();
        try {
            svHealth.stop();
        } catch (Exception e) {
            throw new RuntimeException("[" + serviceInfo.getPath() + "] unsubscribe failed !", e);
        } finally {
            reentrantLock.unlock();
        }
    }

    @Override
    public void register(RegistryServiceInfo serviceInfo) {
        reentrantLock.lock();
        try {
            Registration registration = buildService(serviceInfo);
            client.agentClient().register(registration);
            ttlCheckIds.put(registration.getId(), serviceInfo);
        } catch (Exception e) {
            throw new RuntimeException("[" + serviceInfo.getPath() + "] register failed !", e);
        } finally {
            reentrantLock.unlock();
        }
    }

    @Override
    public void unregister(RegistryServiceInfo serviceInfo) {
        reentrantLock.lock();
        try {
            String serviceId = buildId(serviceInfo);
            ttlCheckIds.remove(serviceId);
            client.agentClient().deregister(serviceId);
        } catch (Exception e) {
            throw new RuntimeException("[" + serviceInfo.getPath() + "] unregister failed !", e);
        } finally {
            reentrantLock.unlock();
        }
    }

    private Registration buildService(RegistryServiceInfo registryServiceInfo) {
        Registration service = ImmutableRegistration.builder()
                .id(buildId(registryServiceInfo))
                .name(buildName(registryServiceInfo))
                .address(registryServiceInfo.getHost())
                .port(registryServiceInfo.getPort())
                .check(Registration.RegCheck.ttl(ttl))
                .tags(buildTags(registryServiceInfo))
                .meta(buildMeta(registryServiceInfo))
                .build();
        return service;
    }

    private Map<String, String> buildMeta(RegistryServiceInfo registryServiceInfo) {
        Map<String, String> metas = new HashMap<>(registryServiceInfo.getParameters());
        metas.remove(PARAM_TAGS);
        return metas;
    }

    private List<String> buildTags(RegistryServiceInfo registryServiceInfo) {
        List<String> tags = new ArrayList<>();
        tags.add(TAG_SERVICE);
        String paramTags = registryServiceInfo.getParameter(PARAM_TAGS);
        if (!StringUtils.isBlank(paramTags)) {
            Arrays.stream(GrpcConstants.COMMA_SPLIT_PATTERN.split(paramTags))
                    .filter(StringUtils::isNotBlank)
                    .collect(Collectors.toCollection(() -> tags));
        }
        return tags;
    }

    private String buildName(RegistryServiceInfo registryServiceInfo) {
        return registryServiceInfo.getNamespace() + "/" + registryServiceInfo.getAlias();
    }

    private String buildId(RegistryServiceInfo registryServiceInfo) {
        return Hashing.murmur3_128()
                .hashString(registryServiceInfo.getFullPath(), StandardCharsets.UTF_8)
                .toString();
    }

    private void checkPass() {
        for (Map.Entry<String, RegistryServiceInfo> entry : ttlCheckIds.entrySet()) {
            String checkId = entry.getKey();
            RegistryServiceInfo registryServiceInfo = entry.getValue();
            try {
                client.agentClient().pass(checkId);
            } catch (Throwable t) {
                LoggerBaseUtil.warn(this, "fail to check pass for url: " + registryServiceInfo.getPath() + ", check id is: " + checkId, t);
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (svHealth != null) {
            svHealth.stop();
        }
        if (ttlConsulCheckExecutor != null) {
            ttlConsulCheckExecutor.shutdown();
        }
        if (client != null) {
            client.destroy();
        }
        ttlCheckIds.clear();
    }

    public static class NamedThreadFactory implements ThreadFactory {
        private final AtomicInteger sequence = new AtomicInteger(1);
        private final String prefix;

        public NamedThreadFactory(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            int seq = sequence.getAndIncrement();
            thread.setName(prefix + (seq > 1 ? "-" + seq : ""));
            thread.setDaemon(true);
            return thread;
        }
    }


}
