package com.snoopy.registry.consul;

import com.snoopy.grpc.base.configure.GrpcRegistryProperties;
import com.snoopy.grpc.base.registry.IRegistry;
import com.snoopy.grpc.base.registry.IRegistryProvider;
import com.snoopy.grpc.base.registry.nacos.NacosRegistry;

/**
 * @author :   kehanjiang
 * @date :   2021/12/1  15:44
 */
public class ConsulRegistryProvider implements IRegistryProvider {
    public static final String REGISTRY_PROTOCOL_CONSUL = "consul";
    private GrpcRegistryProperties grpcRegistryProperties;

    public ConsulRegistryProvider(GrpcRegistryProperties grpcRegistryProperties) {
        this.grpcRegistryProperties = grpcRegistryProperties;
    }

    @Override
    public IRegistry newRegistryInstance() {
        return new NacosRegistry(grpcRegistryProperties);
    }

    @Override
    public String registryType() {
        return REGISTRY_PROTOCOL_CONSUL;
    }
}
