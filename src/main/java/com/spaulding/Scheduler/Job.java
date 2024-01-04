package com.spaulding.Scheduler;

import lombok.NonNull;

public record Job (@NonNull String id, @NonNull String classToRun, @NonNull String[] args) {}
