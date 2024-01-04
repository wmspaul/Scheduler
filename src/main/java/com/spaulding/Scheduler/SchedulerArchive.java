package com.spaulding.Scheduler;

import com.spaulding.tools.Archive.Archive;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class SchedulerArchive extends Archive {
    public static String STATUS_ERROR = "Error";
    public static String STATUS_LOADED = "Loaded";
    public static String STATUS_RUNNING = "Running";
    public static String STATUS_COMPLETED = "Completed";

    public SchedulerArchive(String className, String urlPrefix, String userName, String credentials) throws SQLException {
        super("Scheduler", className, urlPrefix + "Scheduler.db", userName, credentials);
    }

    @Override
    protected void setup() throws SQLException {

    }

    public List<Row> getJobs() {
        return new ArrayList<>();
    }

    public List<Row> getGroupInfo(String groupName) {
        return new ArrayList<>();
    }

    public List<Row> getJobInfo(String jobName) {
        return new ArrayList<>();
    }

    public void updateJobInfo(String id, String status) {

    }

    public void addJobError(String id, String message) {

        updateJobInfo(id, SchedulerArchive.STATUS_ERROR);
    }
}
