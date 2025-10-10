#!/usr/bin/env python3

import os
import sys
import argparse
from collections import defaultdict

# --- Utility Functions ---

def parse_config(logs_dir):
    """
    Reads the generated config file to get N (total processes), M (messages per sender),
    and R (receiver ID). Assumes the config format is 'M R' in the logs directory.
    """
    config_path = os.path.join(logs_dir, "config")
    try:
        with open(config_path, 'r') as f:
            # Format: m i (messages per sender, receiver ID)
            line = f.readline().strip()
            if not line:
                raise ValueError("Config file is empty.")
            parts = line.split()
            if len(parts) != 2:
                raise ValueError(f"Config format is incorrect: Expected 'M R', got '{line}'")

            M = int(parts[0])
            R = int(parts[1])

        # Infer N (total processes) from the generated host files if available,
        # but for robustness, we can try to infer from logs files present.
        # For simplicity and aligning with stress.py args, we'll try to find the max ID.
        N = 0
        for filename in os.listdir(logs_dir):
            if filename.startswith('proc') and filename.endswith('.output'):
                try:
                    # Extracts the ID from 'procXX.output'
                    proc_id = int(filename[4:-7])
                    N = max(N, proc_id)
                except ValueError:
                    continue

        if N == 0:
            raise FileNotFoundError(f"Could not determine total process count (N) from files in {logs_dir}")

        if R < 1 or R > N:
            raise ValueError(f"Receiver ID R={R} is outside the process ID range [1, {N}].")

        print(f"[INFO] Configuration read: N={N} processes, M={M} messages per sender, R=Receiver ID {R}")
        return N, M, R

    except FileNotFoundError:
        print(f"[ERROR] Required file not found: {config_path}. Did you run stress.py first?")
        sys.exit(1)
    except Exception as e:
        print(f"[ERROR] Failed to parse config file: {e}")
        sys.exit(1)


def validate_perfect_links(logs_dir):
    """
    Validates the Perfect Links properties (PL1, PL2, PL3) based on log files.
    """
    try:
        N, M, R = parse_config(logs_dir)
    except SystemExit:
        return 1

    sender_ids = sorted([i for i in range(1, N + 1) if i != R])
    expected_delivery_count = M * (N - 1)
    delivered_messages = set()
    errors = []

    print("-" * 50)
    print(f"Starting Perfect Links Validation for {N} processes.")
    print(f"Receiver ID: {R}. Senders: {sender_ids}")
    print(f"Expected unique delivered messages: {expected_delivery_count}")
    print("-" * 50)

    # --- 1. Validate Sender Logs (PL3: Correct sending) ---
    print("--- 1. Validating Sender Logs (Send Events 'b') ---")
    all_senders_passed = True
    for sender_id in sender_ids:
        filename = os.path.join(logs_dir, f"proc{sender_id:02d}.output")
        current_seq = 0
        log_lines = []

        try:
            with open(filename, 'r') as f:
                log_lines = [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            errors.append(f"Sender {sender_id}: Log file not found at {filename}.")
            all_senders_passed = False
            continue

        # Check total count
        if len(log_lines) < M:
            errors.append(f"Sender {sender_id}: FAILED. Sent {len(log_lines)} messages, expected {M}.")
            all_senders_passed = False
            continue

        # Check sequence and format
        valid_log = True
        for i, line in enumerate(log_lines):
            try:
                parts = line.split()
                if len(parts) != 2 or parts[0] != 'b':
                    errors.append(f"Sender {sender_id}: FAILED. Invalid line format: '{line}' (Expected 'b seq_nr').")
                    valid_log = False
                    break

                seq_nr = int(parts[1])
                expected_seq = i + 1

                if seq_nr != expected_seq:
                    errors.append(f"Sender {sender_id}: FAILED. Sequence error at line {i+1}: Expected 'b {expected_seq}', got '{line}'.")
                    valid_log = False
                    break

            except ValueError:
                errors.append(f"Sender {sender_id}: FAILED. Non-integer sequence number found in line: '{line}'.")
                valid_log = False
                break

        if valid_log:
            print(f"Sender {sender_id}: PASSED (Logged {M} messages 1 to {M} correctly).")

    if not all_senders_passed:
        print("\n[RESULT] FAILED: Sender validation failed.")
        return 1


    # --- 2. Validate Receiver Log (PL1, PL2, PL3) ---
    print("\n--- 2. Validating Receiver Log (Delivery Events 'd') ---")
    receiver_filename = os.path.join(logs_dir, f"proc{R:02d}.output")

    try:
        with open(receiver_filename, 'r') as f:
            log_lines = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        errors.append(f"Receiver {R}: Log file not found at {receiver_filename}.")
        print("\n[RESULT] FAILED: Receiver log not found.")
        return 1

    # Check deliveries against PL properties
    valid_deliveries = True
    for i, line in enumerate(log_lines):
        try:
            parts = line.split()
            if len(parts) != 3 or parts[0] != 'd':
                errors.append(f"Receiver {R} (Line {i+1}): FAILED (PL3/Format). Invalid line format: '{line}' (Expected 'd sender seq_nr').")
                valid_deliveries = False
                continue

            sender_id = int(parts[1])
            seq_nr = int(parts[2])
            message_tuple = (sender_id, seq_nr)

            # PL3: No Creation / Valid Sender
            if sender_id not in sender_ids:
                errors.append(f"Receiver {R} (Line {i+1}): FAILED (PL3: No Creation). Message delivered from invalid sender ID: {sender_id}.")
                valid_deliveries = False
                continue

            # PL3: No Creation / Valid Sequence Number
            if seq_nr < 1 or seq_nr > M:
                errors.append(f"Receiver {R} (Line {i+1}): FAILED (PL3: No Creation). Delivered message with invalid sequence number: {seq_nr} (Expected 1 to {M}).")
                valid_deliveries = False
                continue

            # PL2: No Duplication
            if message_tuple in delivered_messages:
                errors.append(f"Receiver {R} (Line {i+1}): FAILED (PL2: No Duplication). Message already delivered: (Sender={sender_id}, Seq={seq_nr}).")
                valid_deliveries = False
                continue

            delivered_messages.add(message_tuple)

        except ValueError:
            errors.append(f"Receiver {R} (Line {i+1}): FAILED (Format). Non-integer ID or sequence number in line: '{line}'.")
            valid_deliveries = False
            continue

    if not valid_deliveries:
        print("\n[RESULT] FAILED: Receiver validation failed due to format, duplication (PL2), or creation (PL3) errors.")
        return 1

    # PL1: Reliable Delivery (Check total count)
    delivered_count = len(delivered_messages)
    if delivered_count < expected_delivery_count:
        errors.append(f"Receiver {R}: FAILED (PL1: Reliable Delivery). Delivered {delivered_count} unique messages, expected {expected_delivery_count}.")
    elif delivered_count == expected_delivery_count:
        print(f"Receiver {R}: PASSED. Delivered the exact expected number of unique messages ({delivered_count}).")
    else: # Should not happen if PL2 check passed above, but good safeguard
        errors.append(f"Receiver {R}: WARNING/Error. Delivered {delivered_count} messages, which is more than the expected {expected_delivery_count}. This suggests a violation of PL2 or PL3/Creation that was missed or a configuration error.")


    # --- 3. Final Summary ---
    print("-" * 50)
    if not errors:
        print("[RESULT] SUCCESS! All Perfect Links properties checked.")
        return 0
    else:
        print("\nSummary of Errors:")
        for error in errors:
            print(f"- {error}")
        print("\n[RESULT] FAILED: Validation script found errors.")
        return 1


def main():
    parser = argparse.ArgumentParser(
        description="Validate log files for Perfect Links (Milestone 1).",
        formatter_class=argparse.RawTextHelpFormatter
    )

    parser.add_argument(
        "logs_dir",
        help="Directory containing the process output files (procXX.output) and the config file.",
    )

    args = parser.parse_args()

    # The validation script returns 0 on success, non-zero on failure.
    sys.exit(validate_perfect_links(args.logs_dir))


if __name__ == "__main__":
    main()
