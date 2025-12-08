const ConfigType = {
  CLUSTER: "cluster",
} as const;

type ConfigType = typeof ConfigType[keyof typeof ConfigType];

// 10000-19999
const randomPort = () => Math.floor(Math.random() * (19999 - 10000) + 10000);

const DB_CONFIGS = {
  [ConfigType.CLUSTER]: (
    port: number = randomPort(),
    name: string = "ioredis-cluster",
    size: number = 1273741824
  ) => {
    return {
      name: name,
      port: port,
      memory_size: size,
      replication: true,
      eviction_policy: "noeviction",
      sharding: true,
      auto_upgrade: true,
      shards_count: 3,
      module_list: [
        {
          module_args: "",
          module_name: "ReJSON",
        },
        {
          module_args: "",
          module_name: "search",
        },
        {
          module_args: "",
          module_name: "timeseries",
        },
        {
          module_args: "",
          module_name: "bf",
        },
      ],
      oss_cluster: true,
      oss_cluster_api_preferred_ip_type: "external",
      proxy_policy: "all-master-shards",
      shards_placement: "sparse",
      shard_key_regex: [
        {
          regex: ".*\\{(?<tag>.*)\\}.*",
        },
        {
          regex: "(?<tag>.*)",
        },
      ],
    };
  },
};

export const getCreateDatabaseConfig = (
  type: ConfigType,
  options?: { port?: number; name?: string; size?: number }
) => {
  return DB_CONFIGS[type](options?.port, options?.name, options?.size);
};
