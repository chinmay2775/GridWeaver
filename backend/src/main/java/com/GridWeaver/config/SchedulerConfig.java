package com.GridWeaver.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

/**
  Bounded scheduler pool.

  Spring's default scheduler grows unboundedly when a fixed-rate task overruns
  its interval -- observed scheduling-339 within ninety seconds of startup,
  one new thread per 250ms tick. A fixed pool makes overruns queue instead,
  so the cost shows up as tick latency rather than silent thread growth. */
@Configuration
public class SchedulerConfig implements SchedulingConfigurer {

    @Bean
    public ThreadPoolTaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler s = new ThreadPoolTaskScheduler();
        s.setPoolSize(4);
        s.setThreadNamePrefix("gw-sched-");
        s.setRemoveOnCancelPolicy(true);
        return s;
    }

    @Override
    public void configureTasks(ScheduledTaskRegistrar registrar) {
        registrar.setTaskScheduler(taskScheduler());
    }
}