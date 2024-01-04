package com.spaulding.Scheduler;

import com.spaulding.tools.Runner.Runner;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

public class JobExecutor extends Thread {
    private final SchedulerArchive archive;

    private Job job = null;

    @Getter
    @Setter
    private boolean isRunning;

    public JobExecutor(@NonNull SchedulerArchive archive) {
        this.archive = archive;
        isRunning = false;
    }

    public synchronized boolean loadJob(@NonNull Job job) {
        if (this.job == null) {
            archive.updateJobInfo(job.id(), SchedulerArchive.STATUS_LOADED);
            this.job = job;
            return true;
        }
        return false;
    }

    public void run() {
        isRunning = true;

        while (isRunning) {
            if (job != null) {
                String[] commands = new String[job.args().length + 2];
                commands[0] = job.classToRun();
                commands[1] = job.id();
                for (int i = 2; i < commands.length; i++) {
                    commands[2] = job.args()[i - 2];
                }

                try {
                    Runner.runProcess(commands);
                }
                catch (Exception e) {
                    archive.addJobError(job.id(), e.getMessage());
                }

                job = null;
            }

            try {
                sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
