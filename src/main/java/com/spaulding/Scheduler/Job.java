package com.spaulding.Scheduler;

import com.spaulding.tools.Archive.Archive;
import lombok.NonNull;

import java.sql.SQLException;
import java.util.List;

public abstract class Job {
    private final Scheduler scheduler;
    private final SchedulerArchive archive;
    private final JobInfo info;
    private final List<Archive.Row> data;

    public Job(@NonNull Scheduler scheduler, @NonNull SchedulerArchive archive, @NonNull JobInfo info) throws SQLException {
        this.scheduler = scheduler;
        this.archive = archive;
        this.info = info;
        data = archive.getJobData(info.getId());
    }

    public abstract void run();
}
