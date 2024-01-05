package com.spaulding.Scheduler;

import com.spaulding.tools.Runner.Runner;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.lang.reflect.Method;

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
            archive.updateJobInfo(job.getId(), SchedulerArchive.STATUS_LOADED);
            this.jobInfo = job;
            return true;
        }
        return false;
    }

    public void run() {
        isRunning = true;

        while (isRunning) {
            if (jobInfo != null) {
                if (jobInfo.getJarToRun() == null) {
                    String classToRun = jobInfo.getClassToRun();
                    Object obj = scheduler.getObjectInstance(classToRun);
                    if (obj == null) {
                        try {
                            Class<?> c = Class.forName(classToRun);
                            Job job = (Job) c.getDeclaredConstructor(SchedulerArchive.class, JobInfo.class).newInstance(archive, jobInfo);
                            job.run();
                        }
                        catch (Throwable e) {
                            archive.addJobError(jobInfo.getId(), e.getMessage());
                        }
                    }
                    else {
                        try {
                            Method method = obj.getClass().getMethod(jobInfo.getMethodToRun());
                            method.invoke(obj);
                        } catch (Throwable e) {
                            archive.addJobError(jobInfo.getId(), e.getMessage());
                        }
                    }
                }
                else {
                    String[] commands = new String[jobInfo.getArgs().length + 2];
                    commands[0] = jobInfo.getJarToRun();
                    commands[1] = jobInfo.getId();
                    for (int i = 2; i < commands.length; i++) {
                        commands[2] = jobInfo.getArgs()[i - 2];
                    }

                    try {
                        Runner.runProcess(commands);
                    } catch (Exception e) {
                        archive.addJobError(jobInfo.getId(), e.getMessage());
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
