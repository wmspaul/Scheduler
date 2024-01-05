package com.spaulding.Scheduler;

import lombok.NonNull;

public abstract class Job {
    private SchedulerArchive archive;
    private JobInfo jobInfo;

    public Job(@NonNull SchedulerArchive archive, @NonNull JobInfo jobInfo) {
        this.archive = archive;
        this.jobInfo = jobInfo;
    }

    public abstract void run();
}
