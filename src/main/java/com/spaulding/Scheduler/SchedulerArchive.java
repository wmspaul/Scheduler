package com.spaulding.Scheduler;

import com.spaulding.tools.Archive.Archive;
import lombok.NonNull;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class SchedulerArchive extends Archive {
    public static final String JOB_TABLE_SELECT = "jobs" + SELECT_FOLLOWER;
    public static final String JOB_TABLE_INSERT = "jobs" + INSERT_FOLLOWER;
    public static final String JOB_TABLE_UPDATE = "jobs" + UPDATE_FOLLOWER;
    public static final String JOB_DATA_TABLE_SELECT = "job-data" + SELECT_FOLLOWER;
    public static final String JOB_DATA_TABLE_INSERT = "job-data" + INSERT_FOLLOWER;
    public static final String JOB_DATA_TABLE_UPDATE = "job-data" + UPDATE_FOLLOWER;
    public static final String GROUP_PROPERTIES_TABLE_SELECT = "group-properties" + SELECT_FOLLOWER;
    public static final String JOB_PROPERTIES_TABLE_SELECT = "job-properties" + SELECT_FOLLOWER;

    public static final String STATUS_ERROR = "Error";
    public static final String STATUS_WAITING = "Waiting";
    public static final String STATUS_LOADED = "Loaded";
    public static final String STATUS_RUNNING = "Running";
    public static final String STATUS_COMPLETED = "Completed";

    public SchedulerArchive(String className, String urlPrefix, String userName, String credentials) throws SQLException {
        super("Scheduler", className, urlPrefix + "Scheduler.db", userName, credentials);
    }

    @Override
    protected void setup() throws SQLException {
        jdbc.execute("CREATE TABLE IF NOT EXISTS jobs (id INTEGER PRIMARY KEY, status VARCHAR, group_name VARCHAR, job_name VARCHAR, last_updated VARCHAR, FOREIGN KEY(group_name) REFERENCES group_properties(name)), FOREIGN KEY(job_name) REFERENCES job_properties(name)))");
        jdbc.execute("CREATE TABLE IF NOT EXISTS group_properties (name VARCHAR, key VARCHAR, value VARCHAR, UNIQUE(id, key))");
        jdbc.execute("CREATE TABLE IF NOT EXISTS job_properties (name VARCHAR, key VARCHAR, value VARCHAR, UNIQUE(id, key))");
        jdbc.execute("CREATE TABLE IF NOT EXISTS job_data (id VARCHAR, key VARCHAR, value VARCHAR, UNIQUE(id, key))");

        Object[] args = new Object[] {
                JOB_TABLE_SELECT,
                "SELECT * FROM jobs WHERE status = ?",
                1,
                "String"
        };
        execute(SYSADMIN, AUTH_REF_TABLE_INSERT, args);

        args = new Object[] {
                JOB_TABLE_INSERT,
                "INSERT INTO jobs VALUES(NULL, '" + STATUS_WAITING + "', ?, ?, ?)",
                3,
                "String,String,LocalDateTime"
        };
        execute(SYSADMIN, AUTH_REF_TABLE_INSERT, args);

        args = new Object[] {
                JOB_TABLE_UPDATE,
                "UPDATE jobs SET status = ?, last_updated = ? WHERE id = ?",
                3,
                "String,LocalDateTime,Integer"
        };
        execute(SYSADMIN, AUTH_REF_TABLE_INSERT, args);

        args = new Object[] {
                JOB_DATA_TABLE_SELECT,
                "SELECT * FROM job_data WHERE id = ?",
                1,
                "Integer"
        };
        execute(SYSADMIN, AUTH_REF_TABLE_INSERT, args);

        args = new Object[] {
                JOB_DATA_TABLE_INSERT,
                "INSERT INTO job_data VALUES(?, ?, ?)",
                3,
                "String,String,String"
        };
        execute(SYSADMIN, AUTH_REF_TABLE_INSERT, args);

        args = new Object[] {
                JOB_DATA_TABLE_UPDATE,
                "UPDATE job_data SET value = ? WHERE id = ? AND key = ?",
                3,
                "String,Integer,String"
        };
        execute(SYSADMIN, AUTH_REF_TABLE_INSERT, args);

        args = new Object[] {
                GROUP_PROPERTIES_TABLE_SELECT,
                "SELECT * FROM group_properties WHERE name = ?",
                1,
                "String"
        };
        execute(SYSADMIN, AUTH_REF_TABLE_INSERT, args);

        args = new Object[] {
                JOB_PROPERTIES_TABLE_SELECT,
                "SELECT * FROM job_properties WHERE name = ?",
                1,
                "String"
        };
        execute(SYSADMIN, AUTH_REF_TABLE_INSERT, args);
    }

    public List<Row> getJobs() throws SQLException {
        return execute(SYSADMIN, JOB_TABLE_SELECT, new Object[]{ STATUS_WAITING });
    }

    public List<Row> getProperties(@NonNull String groupName, String jobName) throws SQLException {
        List<Row> groupProperties = getGroupProperties(groupName);
        if (jobName == null) {
            return groupProperties;
        }

        List<Row> jobProperties = getJobProperties(jobName);
        List<Row> results = new ArrayList<>(jobProperties);
        for (Row groupProperty : groupProperties) {
            String key = (String) groupProperty.getResult(1);
            boolean found = false;
            for (Row jobProperty : jobProperties) {
                String check = (String) jobProperty.getResult(1);
                if (key.equals(check)) {
                    found = true;
                    break;
                }
            }

            if (!found) {
                results.add(groupProperty);
            }
        }

        return results;
    }

    private List<Row> getGroupProperties(@NonNull String groupName) throws SQLException {
        return execute(SYSADMIN, GROUP_PROPERTIES_TABLE_SELECT, new Object[]{ groupName });
    }

    private List<Row> getJobProperties(@NonNull String jobName) throws SQLException {
        return execute(SYSADMIN, JOB_PROPERTIES_TABLE_SELECT, new Object[]{ jobName });
    }

    public List<Row> getJobData(@NonNull Integer id) throws SQLException {
        return execute(SYSADMIN, JOB_DATA_TABLE_SELECT, new Object[]{ id });
    }

    public void updateJobStatus(@NonNull Integer id, @NonNull String status) throws SQLException {
        execute(SYSADMIN, JOB_TABLE_UPDATE, new Object[]{ status, LocalDateTime.now(), id });
    }

    public void addJobError(@NonNull Integer id, @NonNull String message) throws SQLException {
        try {
            execute(SYSADMIN, JOB_DATA_TABLE_INSERT, new Object[]{ id, "error", message });
        }
        catch (SQLException e) {
            execute(SYSADMIN, JOB_DATA_TABLE_UPDATE, new Object[]{ message, id, "error" });
        }
        updateJobStatus(id, SchedulerArchive.STATUS_ERROR);
    }

    public void addJob() {

    }
}
