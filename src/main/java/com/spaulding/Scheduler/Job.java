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

    public void start() throws SQLException {
        archive.updateJobStatus(info.getId(), SchedulerArchive.STATUS_RUNNING);
        try {
            run();
            archive.updateJobStatus(info.getId(), SchedulerArchive.STATUS_COMPLETED);
        }
        catch (Exception e) {
            archive.updateJobStatus(info.getId(), SchedulerArchive.STATUS_ERROR);
            // TODO: Error handling config through database
        }
    }

    protected abstract void run();
}
