package com.github.hpchugo.redis;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.api.sync.RedisTransactionalCommands;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;

@Controller("/time")
public class RateLimitedTimeEndpoint {
    private static final Logger LOG = LoggerFactory.getLogger(RateLimitedTimeEndpoint.class);
    private static final int QUOTA_PER_MINUTE = 10;
    private static final String SYNC = "sync";
    private static final String ASYNC = "async";

    private final StatefulRedisConnection<String, String> redis;
    private Map<Class<?>, String> map = new HashMap<Class<?>, String>();

    public RateLimitedTimeEndpoint(StatefulRedisConnection<String, String> redis) {
        this.redis = redis;
    }

    @Get("/")
    public String time(){
        return getTime("EXAMPLE::TIME", LocalTime.now(), SYNC);
    }

    @Get("/utc")
    public String utc(){
        return getTime("EXAMPLE::UTC", LocalTime.now(Clock.systemUTC()), SYNC);
    }

    @ExecuteOn(TaskExecutors.IO)
    @Get("/async")
    public String timeAsync(){
        return getTime("EXAMPLE::TIME", LocalTime.now(), ASYNC);
    }

    @ExecuteOn(TaskExecutors.IO)
    @Get("/async/utc")
    public String utcAsync(){
        return getTime("EXAMPLE::UTC", LocalTime.now(Clock.systemUTC()), ASYNC);
    }

    private String getTime(String key, final LocalTime now, String keyMap) {

        final String value = redis.sync().get(key);
        int currentQuota = null == value ? 0 : Integer.parseInt(value);
        if(currentQuota >= QUOTA_PER_MINUTE){
            final String err = String.format("Rate limit reached %s %s/%s", key, currentQuota, QUOTA_PER_MINUTE);
            LOG.info(err);
            return err;
        }
        LOG.info("Current quota {} in {}/{}", key, currentQuota, QUOTA_PER_MINUTE);
        if (keyMap.equalsIgnoreCase(SYNC)) {
            increaseCurrentQuota(key);
        } else {
            increaseCurrentQuotaAsync(key);
        }
        return now.toString();
    }

    private void increaseCurrentQuota(final String key){
        RedisCommands commands = redis.sync();
        commands.multi();
        commands.incrby(key, 1);
        var remainingSeconds = 60 - LocalTime.now().getSecond();
        commands.expire(key, remainingSeconds);
        commands.exec();
    }

    private void increaseCurrentQuotaAsync(final String key) {
        final RedisAsyncCommands<String, String> commands = redis.async();
        commands.multi();
        commands.incrby(key, 1);
        var remainingSeconds = 60 - LocalTime.now().getSecond();
        commands.expire(key, remainingSeconds);
        commands.exec();
    }
}
