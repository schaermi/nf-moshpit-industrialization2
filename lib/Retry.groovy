/**
 * Shared errorStrategy helper: pauses before a failed task is resubmitted.
 *
 * The cluster intermittently hits transient Apptainer/squashfuse container-mount
 * timeouts under storage/FUSE contention; retrying immediately tends to hit the
 * same contention again. Sleeping before resubmission gives it time to clear.
 */
class Retry {
    static String withDelay(String decision, int delaySeconds) {
        if (decision == 'retry') {
            sleep(delaySeconds * 1000L)
        }
        return decision
    }
}
