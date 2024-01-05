package com.spaulding.Scheduler;

import lombok.Getter;
import lombok.NonNull;

@Getter
public class JobInfo {
    private final Integer id;
    private final String jarToRun, classToRun, methodToRun;
    private final String[] args;
    public JobInfo(@NonNull Integer id, @NonNull String classToRun, String methodToRun, @NonNull String[] args) {
        this.id = id;
        jarToRun = null;
        this.classToRun = classToRun;
        this.methodToRun = methodToRun;
        this.args = args;
    }

    public JobInfo(@NonNull Integer id, @NonNull String jarToRun, @NonNull String[] args) {
        this.id = id;
        this.jarToRun = jarToRun;
        classToRun = null;
        methodToRun = null;
        this.args = args;
    }
}
