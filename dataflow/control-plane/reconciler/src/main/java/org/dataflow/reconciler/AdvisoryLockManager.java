package org.dataflow.reconciler;

import org.dataflow.domain.tract.TractName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.zip.CRC32;

@Component
public class AdvisoryLockManager {

    private static final Logger log = LoggerFactory.getLogger(AdvisoryLockManager.class);
    // CRC32("dataflow") truncated to int — used as the first key of
    // pg_try_advisory_lock(int, int) so locks here cannot collide with
    // unrelated advisory locks taken elsewhere in the database.
    private static final int LOCK_NAMESPACE = 0xD47AF10A;

    private final JdbcTemplate jdbc;

    public AdvisoryLockManager(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public boolean tryLock(TractName tract) {
        Boolean acquired = jdbc.queryForObject(
                "SELECT pg_try_advisory_lock(?, ?)",
                Boolean.class, LOCK_NAMESPACE, keyOf(tract));
        return acquired != null && acquired;
    }

    public void unlock(TractName tract) {
        Boolean released = jdbc.queryForObject(
                "SELECT pg_advisory_unlock(?, ?)",
                Boolean.class, LOCK_NAMESPACE, keyOf(tract));
        if (released == null || !released) {
            log.warn("Advisory unlock returned false for tract {}", tract);
        }
    }

    public <T> T withLock(TractName tract, java.util.function.Supplier<T> work) {
        if (!tryLock(tract)) {
            throw new LockNotAcquiredException(tract);
        }
        try {
            return work.get();
        } finally {
            unlock(tract);
        }
    }

    private int keyOf(TractName tract) {
        CRC32 crc = new CRC32();
        crc.update(tract.value().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return (int) crc.getValue();
    }

    public static class LockNotAcquiredException extends RuntimeException {
        public LockNotAcquiredException(TractName tract) {
            super("Advisory lock not acquired for tract " + tract);
        }
    }
}
