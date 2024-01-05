package com.spaulding.Scheduler;

import com.spaulding.tools.Runner.Runner;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;
import java.sql.SQLException;

@Slf4j
public class JobExecutor extends Thread {
    private final Scheduler scheduler;
    private final SchedulerArchive archive;

    private JobInfo jobInfo = null;

    @Getter
    @Setter
    private boolean isRunning;

    public JobExecutor(@NonNull Scheduler scheduler, @NonNull SchedulerArchive archive) {
        this.scheduler = scheduler;
        this.archive = archive;
        isRunning = false;
    }

    public synchronized boolean loadJob(@NonNull JobInfo job) {
        if (this.jobInfo == null) {
            try {
                archive.updateJobStatus(job.getId(), SchedulerArchive.STATUS_LOADED);
            } catch (SQLException e) {
                log.error(e.getMessage());
                throw new RuntimeException(e);
            }
            this.jobInfo = job;
            return true;
        }
        return false;
    }

    public void run() {
        isRunning = true;

        while (isRunning) {
            if (jobInfo != null) {
                try {
                    if (jobInfo.getJarToRun() == null) {
                        String classToRun = jobInfo.getClassToRun();
                        Object obj = scheduler.getObjectInstance(classToRun);
                        if (obj == null) {
                            Class<?> c = Class.forName(classToRun);
                            Job job = (Job) c.getDeclaredConstructor(
                                            Scheduler.class,
                                            SchedulerArchive.class,
                                            JobInfo.class)
                                    .newInstance(scheduler, archive, jobInfo);
                            job.start();
                        } else {
                            Method method = obj.getClass().getMethod(jobInfo.getMethodToRun());
                            method.invoke(obj);
                        }
                    } else {
                        String[] commands = new String[jobInfo.getArgs().length + 2];
                        commands[0] = jobInfo.getJarToRun();
                        commands[1] = jobInfo.getId() + "";
                        for (int i = 2; i < commands.length; i++) {
                            commands[2] = jobInfo.getArgs()[i - 2];
                        }

                        Runner.runProcess(commands);
                    }
                }
                catch (Throwable e) {
                    try {
                        archive.addJobError(jobInfo.getId(), e.getMessage());
                    } catch (SQLException ex) {
                        log.error(ex.getMessage());
                        throw new RuntimeException(ex);
                    }
                }

                jobInfo = null;
            }

            try {
                sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
