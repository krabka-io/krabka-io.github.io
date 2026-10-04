// Copyright 2026 The Krabka Authors. SPDX-License-Identifier: Apache-2.0
public final class OpenMessagingMain {
    public static void main(String[] args) throws Exception {
        io.openmessaging.benchmark.Benchmark.main(args);
        // KafkaTopicCreator leaves a non-daemon scheduler alive after main
        // returns. Exit only after upstream has finished and closed its worker.
        // The runner independently rejects missing/failed result files.
        System.exit(0);
    }
}
