package com.spaulding.Scheduler;

import com.spaulding.tools.Archive.Archive;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class Scheduler extends Thread {
    private final Map<String, Object> objectInstances = new HashMap<>();
    private final SchedulerArchive archive;
    private final JobExecutor[] jobExecutors;
    private boolean isRunning;

    public Scheduler(@NonNull String className, @NonNull String urlPrefix, @NonNull String user, @NonNull String credentials, @NonNull Integer threads) throws SQLException {
        archive = new SchedulerArchive(className, urlPrefix, user, credentials);
        jobExecutors = new JobExecutor[threads];

        for (int i = 0; i < threads; i++) {
            jobExecutors[i] = new JobExecutor(this, archive);
        }

        isRunning = false;
    }

    public synchronized void registerObjectInstance(String name, Object object) {
        objectInstances.putIfAbsent(name, object);
    }

    public Object getObjectInstance(String name) {
        return objectInstances.get(name);
    }

    public void run() {
        isRunning = true;

        for (JobExecutor executor : jobExecutors) {
            if (!executor.isRunning()) {
                executor.start();
            }
        }

        while (isRunning) {
            try {
                List<JobInfo> jobs = getJobs();
                while (!jobs.isEmpty()) {
                    for (JobExecutor executor : jobExecutors) {
                        JobInfo job = jobs.get(0);
                        if (executor.loadJob(job)) {
                            jobs.remove(0);
                        }
                    }

                    if (jobs.isEmpty()) {
                        jobs = getJobs();
                    }
                }
            }
            catch (SQLException e) {
                log.error(e.getMessage());
                throw new RuntimeException(e);
            }

            try {
                sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private List<JobInfo> getJobs() throws SQLException {
        List<Archive.Row> rows = archive.getJobs();
        List<JobInfo> jobs = new ArrayList<>();
        for (Archive.Row row : rows) {
            Integer id = (Integer) row.getResult(0);
            String groupName = (row.getResult(2) == null) ? null : (String) row.getResult(2);
            String jobName = (row.getResult(3) == null) ? null : (String) row.getResult(2);
            List<Archive.Row> properties = archive.getProperties(groupName, jobName);
            String jarToRun = null;
            String classToRun = null;
            String methodToRun = null;
            boolean fail = false;
            int jarToRunInstances = 0;
            int classToRunInstances = 0;
            int methodToRunInstances = 0;
            int unknownPropertyInstances = 0;
            int duplicateArgInstances = 0;
            int unknownArgNumberInstances = 0;
            for (Archive.Row info : properties) {
                String key = (String) info.getResult(1);
                String value = (String) info.getResult(2);
                if (key.equals("classToRun")) {
                    if (classToRun == null) {
                        classToRun = value;
                    }
                    else {
                        fail = true;
                    }
                    classToRunInstances++;
                }
                else if (key.equals("jarToRun")) {
                    if (jarToRun == null) {
                        jarToRun = value;
                    }
                    else {
                        fail = true;
                    }
                    jarToRunInstances++;
                }
                else if (key.equals("methodToRun")) {
                    if (methodToRun == null) {
                        methodToRun = value;
                    }
                    else {
                        fail = true;
                    }
                    methodToRunInstances++;
                }
                else if (!key.contains("arg")) {
                    fail = true;
                    unknownPropertyInstances++;
                }
            }

            String[] args = new String[properties.size() - (classToRunInstances + jarToRunInstances + methodToRunInstances)];
            for (Archive.Row info : properties) {
                String key = (String) info.getResult(1);
                String value = (String) info.getResult(2);

                if (key.contains("arg")) {
                    String[] argInfo = key.split("-");
                    int argNum = 0;
                    if (argInfo.length == 2) {
                        try {
                            argNum = Integer.parseInt(argInfo[1]);
                        }
                        catch (NumberFormatException e) {
                            fail = true;
                            unknownArgNumberInstances++;
                        }
                    }
                    else if (argInfo.length > 2) {
                        fail = true;
                        unknownArgNumberInstances++;
                    }

                    if (argNum >= args.length) {
                        fail = true;
                        unknownArgNumberInstances++;
                    }
                    else if (args[argNum] == null) {
                        args[argNum] = value;
                    }
                    else {
                        fail = true;
                        duplicateArgInstances++;
                    }
                }
            }

            if (classToRunInstances == 0) {
                fail = true;
            }

            fail = fail || (jarToRun == null && classToRun == null);

            if (fail) {
                String message = "Property errors found, please double check group and job properties: " +
                        ((jarToRunInstances != 0 && (classToRunInstances != 0 || methodToRunInstances != 0)) ?
                                "Found use of both jar run properties and class run properties." :
                                "") +
                        ((classToRunInstances == 0 && jarToRunInstances == 0) ? "Could not find classToRun property." :
                                ((classToRunInstances == 1) ? "" : classToRunInstances + " instances of classToRun property found.")
                        ) +
                        ((methodToRunInstances == 1) ? "" : methodToRunInstances + " instances of methodToRun property found.") +
                        ((duplicateArgInstances == 0) ? "" : duplicateArgInstances + " instance(s) of duplicate argument properties found.") +
                        ((unknownArgNumberInstances == 0) ? "" : unknownArgNumberInstances + " instance(s) of an unknown argument property number found.") +
                        ((unknownPropertyInstances == 0) ? "" : unknownPropertyInstances + " instance(s) of an unknown property found.");

                try {
                    archive.addJobError(id, message);
                } catch (SQLException e) {
                    log.error(e.getMessage());
                    throw new RuntimeException(e);
                }
            }
            else {
                JobInfo job;
                if (jarToRun == null) {
                    job = new JobInfo(id, classToRun, methodToRun, args);
                }
                else {
                    job = new JobInfo(id, jarToRun, args);
                }
                jobs.add(job);
            }
        }

        return jobs;
    }

    public void shutdown() {
        for (JobExecutor executor : jobExecutors) {
            executor.setRunning(false);
        }

        isRunning = false;
    }
}
