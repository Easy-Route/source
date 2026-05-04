package org.dataflow.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Генератор нагрузки демонстрационного интернет-магазина.
 * Имитирует операции INSERT (60 %), UPDATE (30 %) и DELETE (10 %)
 * над таблицами customers / orders / order_items / products.
 *
 * Параметры командной строки:
 *   --url=jdbc:postgresql://localhost:5432/shop
 *   --user=dataflow_cdc
 *   --password=dataflow_cdc
 *   --rate=10000        целевое число операций в секунду
 *   --duration=300      длительность работы в секундах (0 — бесконечно)
 *   --threads=4         число параллельных воркеров
 */
public final class LoadGenerator {

    private static final Logger log = LoggerFactory.getLogger(LoadGenerator.class);

    public static void main(String[] args) throws Exception {
        Args parsed = Args.parse(args);
        try (Connection seed = DriverManager.getConnection(parsed.url, parsed.user, parsed.password)) {
            ensureProducts(seed);
        }

        AtomicLong inserted = new AtomicLong();
        AtomicLong updated = new AtomicLong();
        AtomicLong deleted = new AtomicLong();
        AtomicLong errors = new AtomicLong();

        long perThread = Math.max(1L, parsed.rate / parsed.threads);
        long delayNanos = TimeUnit.SECONDS.toNanos(1) / Math.max(1L, perThread);

        ScheduledExecutorService reporter = Executors.newSingleThreadScheduledExecutor();
        reporter.scheduleAtFixedRate(() -> log.info(
                "ops/s ~ {} (insert={} update={} delete={} errors={})",
                inserted.get() + updated.get() + deleted.get(),
                inserted.get(), updated.get(), deleted.get(), errors.get()
        ), 1, 5, TimeUnit.SECONDS);

        long deadline = parsed.durationSec > 0
                ? System.nanoTime() + TimeUnit.SECONDS.toNanos(parsed.durationSec)
                : Long.MAX_VALUE;

        Thread[] workers = new Thread[parsed.threads];
        for (int i = 0; i < workers.length; i++) {
            workers[i] = new Thread(() -> {
                try (Connection c = DriverManager.getConnection(parsed.url, parsed.user, parsed.password)) {
                    runWorker(c, deadline, delayNanos, inserted, updated, deleted, errors);
                } catch (SQLException e) {
                    log.error("Worker bailed out", e);
                }
            }, "load-" + i);
            workers[i].start();
        }
        for (Thread t : workers) {
            t.join();
        }
        reporter.shutdownNow();
        log.info("done — total {} ops",
                inserted.get() + updated.get() + deleted.get());
    }

    private static void runWorker(Connection c, long deadlineNanos, long delayNanos,
                                  AtomicLong ins, AtomicLong upd, AtomicLong del, AtomicLong errs) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        while (System.nanoTime() < deadlineNanos) {
            int op = rnd.nextInt(100);
            try {
                if (op < 60) {
                    insertOrder(c);
                    ins.incrementAndGet();
                } else if (op < 90) {
                    updateOrder(c);
                    upd.incrementAndGet();
                } else {
                    deleteOrder(c);
                    del.incrementAndGet();
                }
            } catch (SQLException e) {
                errs.incrementAndGet();
            }
            sleepNanos(delayNanos);
        }
    }

    private static void insertOrder(Connection c) throws SQLException {
        long customerId = ensureCustomer(c);
        try (PreparedStatement ps = c.prepareStatement(
                "INSERT INTO public.orders(customer_id, status, total_cents) VALUES (?, 'NEW', ?) RETURNING id")) {
            ps.setLong(1, customerId);
            ps.setInt(2, ThreadLocalRandom.current().nextInt(1_000, 500_000));
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                long orderId = rs.getLong(1);
                appendItems(c, orderId);
            }
        }
    }

    private static void updateOrder(Connection c) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "UPDATE public.orders SET status = ?, updated_at = now() "
                        + "WHERE id = (SELECT id FROM public.orders ORDER BY random() LIMIT 1)")) {
            ps.setString(1, ThreadLocalRandom.current().nextBoolean() ? "PAID" : "SHIPPED");
            ps.executeUpdate();
        }
    }

    private static void deleteOrder(Connection c) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "DELETE FROM public.orders WHERE id = (SELECT id FROM public.orders ORDER BY random() LIMIT 1)")) {
            ps.executeUpdate();
        }
    }

    private static void appendItems(Connection c, long orderId) throws SQLException {
        int count = ThreadLocalRandom.current().nextInt(1, 4);
        try (PreparedStatement ps = c.prepareStatement(
                "INSERT INTO public.order_items(order_id, product_id, qty, price_cents) "
                        + "SELECT ?, id, ?, price_cents FROM public.products ORDER BY random() LIMIT ?")) {
            ps.setLong(1, orderId);
            ps.setInt(2, ThreadLocalRandom.current().nextInt(1, 5));
            ps.setInt(3, count);
            ps.executeUpdate();
        }
    }

    private static long ensureCustomer(Connection c) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "INSERT INTO public.customers(email, full_name) "
                        + "VALUES (?, ?) ON CONFLICT (email) DO UPDATE SET updated_at = now() RETURNING id")) {
            String suffix = Long.toHexString(ThreadLocalRandom.current().nextLong());
            ps.setString(1, "user-" + suffix + "@example.com");
            ps.setString(2, "Customer " + suffix);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    private static void ensureProducts(Connection c) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT COUNT(*) FROM public.products")) {
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                if (rs.getInt(1) > 0) {
                    return;
                }
            }
        }
        List<String> seeds = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            seeds.add(String.format("('SKU-%05d', 'Product %d', %d)",
                    i, i, ThreadLocalRandom.current().nextInt(1_000, 250_000)));
        }
        try (PreparedStatement ps = c.prepareStatement(
                "INSERT INTO public.products(sku, title, price_cents) VALUES "
                        + String.join(",", seeds))) {
            ps.executeUpdate();
        }
    }

    private static void sleepNanos(long nanos) {
        if (nanos <= 0) {
            return;
        }
        long deadline = System.nanoTime() + nanos;
        while (System.nanoTime() < deadline) {
            // busy-wait is acceptable for the highest target rates; at low
            // rates LockSupport.parkNanos would be more polite.
            Thread.onSpinWait();
        }
    }

    private record Args(String url, String user, String password,
                        long rate, long durationSec, int threads) {

        static Args parse(String[] argv) {
            String url = "jdbc:postgresql://localhost:5432/shop";
            String user = "dataflow_cdc";
            String password = "dataflow_cdc";
            long rate = 10_000;
            long durationSec = 300;
            int threads = 4;
            for (String a : argv) {
                String[] kv = a.split("=", 2);
                if (kv.length != 2) {
                    continue;
                }
                switch (kv[0]) {
                    case "--url" -> url = kv[1];
                    case "--user" -> user = kv[1];
                    case "--password" -> password = kv[1];
                    case "--rate" -> rate = Long.parseLong(kv[1]);
                    case "--duration" -> durationSec = Long.parseLong(kv[1]);
                    case "--threads" -> threads = Integer.parseInt(kv[1]);
                    default -> {}
                }
            }
            return new Args(url, user, password, rate, durationSec, threads);
        }
    }
}
