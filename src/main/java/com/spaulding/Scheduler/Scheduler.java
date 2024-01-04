package com.spaulding.Scheduler;

import com.spaulding.tools.Archive.Archive;
import lombok.NonNull;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Scheduler extends Thread {
    private final SchedulerArchive archive;
    private final JobExecutor[] jobExecutors;
    private boolean isRunning;

    public Scheduler(@NonNull String className, @NonNull String urlPrefix, @NonNull String user, @NonNull String credentials, @NonNull Integer threads) throws SQLException {
        archive = new SchedulerArchive(className, urlPrefix, user, credentials);
        jobExecutors = new JobExecutor[threads];

        for (int i = 0; i < threads; i++) {
            jobExecutors[i] = new JobExecutor(archive);
        }

        isRunning = false;
    }

    public void run() {
        isRunning = true;

        for (JobExecutor executor : jobExecutors) {
            if (!executor.isRunning()) {
                executor.start();
            }
        }

        while (isRunning) {
            List<Job> jobs = getJobs();
            while (!jobs.isEmpty()) {
                for (JobExecutor executor : jobExecutors) {
                    Job job = jobs.get(0);
                    if (executor.loadJob(job)) {
                        jobs.remove(0);
                    }
                }

                if (jobs.isEmpty()) {
                    jobs = getJobs();
                }
            }

            try {
                sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private List<Job> getJobs() {
        List<Archive.Row> rows = archive.getJobs();
        List<Job> jobs = new ArrayList<>();
        for (Archive.Row row : rows) {
            String id = (String) row.getResult(0);
            String groupName = (String) row.getResult(2);
            List<Archive.Row> groupInfo = archive.getGroupInfo(groupName);
            String classToRun = null;
            String[] args = new String[groupInfo.size() - 1];
            boolean fail = false;
            int classToRunInstances = 0;
            int unknownPropertyInstances = 0;
            int duplicateArgInstances = 0;
            int unknownArgNumberInstances = 0;
            for (Archive.Row info : groupInfo) {
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
                else if (key.contains("arg")) {
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
                else {
                    fail = true;
                    unknownPropertyInstances++;
                }
            }

            if (classToRunInstances == 0) {
                fail = true;
            }

            if (fail) {
                String message = "Group Name Errors: " +
                        ((classToRunInstances == 0) ? "Could not find classToRun property." :
                                ((classToRunInstances == 1) ? "" : classToRunInstances + " instances of classToRun property found.")
                        ) +
                        ((duplicateArgInstances == 0) ? "" : duplicateArgInstances + " instance(s) of duplicate arguments found.") +
                        ((unknownArgNumberInstances == 0) ? "" : unknownArgNumberInstances + " instance(s) of an unknown arg number found.") +
                        ((unknownPropertyInstances == 0) ? "" : unknownPropertyInstances + " instance(s) of an unknown property found.");
                archive.addJobError(id, message);
            }
            else {
                jobs.add(new Job(id, classToRun, args));
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
