"""Single source of truth for problem states and aliases.

Merged from handlers.py (superset) and main.py.
"""

PROBLEM_STATES = {
    "CrashLoopBackOff", "OOMKilled", "ImagePullBackOff",
    "ErrImagePull", "Error", "CreateContainerError",
    "CreateContainerConfigError", "ContainerStatusUnknown",
    "ErrImageNeverPull",
}

# Pod-level status reasons (pod.status.reason, not container state)
POD_PROBLEM_REASONS = {
    "Evicted", "OutOfcpu", "OutOfmemory", "NodeAffinity",
    "UnexpectedAdmissionError",
}

IMPORTANT_EVENT_REASONS = {
    # Scheduling
    "FailedScheduling",
    # Image pull
    "Failed", "BackOff", "ErrImageNeverPull", "InspectFailed",
    # Container lifecycle
    "Unhealthy", "ProbeWarning", "ExceededGracePeriod",
    "FailedPostStartHook", "FailedPreStopHook",
    # Pod sandbox / creation
    "FailedCreatePodSandBox", "FailedCreatePodContainer",
    "FailedKillPod", "NetworkNotReady", "FailedSync",
    # Volume / mount
    "FailedMount", "FailedAttachVolume", "FailedMapVolume",
    "VolumeResizeFailed", "FileSystemResizeFailed",
    # Node-level
    "Evicted", "NodeNotReady", "Rebooted", "Shutdown",
    "FreeDiskSpaceFailed", "ContainerGCFailed", "ImageGCFailed",
    # Validation
    "FailedValidation",
}

PROBLEM_ALIASES = {
    "BackOff": "crash",
    "CrashLoopBackOff": "crash",
    "Failed": "error",
    "OOMKilled": "oom",
    "ImagePullBackOff": "image-pull",
    "ErrImagePull": "image-pull",
    "ErrImageNeverPull": "image-pull",
    "InspectFailed": "image-pull",
    "Error": "error",
    "CreateContainerError": "create-error",
    "CreateContainerConfigError": "create-error",
    "ContainerStatusUnknown": "error",
    "FailedCreatePodSandBox": "create-error",
    "FailedCreatePodContainer": "create-error",
    "FailedKillPod": "error",
    "NetworkNotReady": "scheduling",
    "FailedSync": "error",
    "FailedScheduling": "scheduling",
    "Unhealthy": "unhealthy",
    "ProbeWarning": "unhealthy",
    "ExceededGracePeriod": "error",
    "FailedPostStartHook": "error",
    "FailedPreStopHook": "error",
    "FailedMount": "mount",
    "FailedAttachVolume": "mount",
    "FailedMapVolume": "mount",
    "VolumeResizeFailed": "mount",
    "FileSystemResizeFailed": "mount",
    "Evicted": "evicted",
    "NodeNotReady": "notready",
    "Rebooted": "error",
    "Shutdown": "error",
    "FreeDiskSpaceFailed": "evicted",
    "ContainerGCFailed": "error",
    "ImageGCFailed": "error",
    "FailedValidation": "error",
    # Pod-level reasons
    "OutOfcpu": "evicted",
    "OutOfmemory": "oom",
    "NodeAffinity": "scheduling",
    "UnexpectedAdmissionError": "error",
}

# Container status reasons used for matching (membership test only)
CONTAINER_STATUS_REASONS = {
    "CrashLoopBackOff", "OOMKilled", "ImagePullBackOff", "ErrImagePull",
    "CreateContainerError", "CreateContainerConfigError",
    "ContainerStatusUnknown", "ErrImageNeverPull", "Error",
}
