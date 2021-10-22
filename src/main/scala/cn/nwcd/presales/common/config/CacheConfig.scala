package cn.nwcd.presales.common.config

import java.time.Duration

case class CacheConfig(cache_write_expire_duration: Duration, cache_max_size: Int)
