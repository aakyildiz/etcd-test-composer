#!/usr/bin/env -S python3 -u
"""
CAS (Compare-And-Swap) atomicity check.

Start three concurrent threads to race to CAS the same key. etcd must guarantee at most one thread wins.

Assertions:
  - always: at most one CAS writer wins  (atomicity — must always hold)
"""

from antithesis.assertions import always
import sys
import threading
import time

sys.path.append("/opt/antithesis/resources")
import helper


CAS_KEY = "/antithesis/cas-check"
INITIAL_VAL = 0
NUM_ROUNDS = 100

def attempt_cas(client, expected_val, new_val, results, idx, start_barrier):
    """
    This function will attempt to CAS the key. 
    successful, it will set the result for the thread to True or False for unsuccessful.
    """
    try:
        start_barrier.wait(timeout=5)
        transaction_result, _ = client.transaction(
            compare=[
                client.transactions.value(CAS_KEY) == str(expected_val).encode()
            ],
            success=[
                client.transactions.put(CAS_KEY, str(new_val))
            ],
            failure=[]
        )
        results[idx] = bool(transaction_result)
    except Exception as e:
        print(f"[cas_check] thread-{idx} error: {e}")
        results[idx] = False


def run_cas_check():
    """
    This function will run the CAS check.
    It will create 3 clients and a verifier client.
    It will then run the CAS check for the number of rounds specified.
    """
    print(f"[cas_check] START rounds={NUM_ROUNDS} threads=3 key={CAS_KEY}", flush=True)

    # Create 3 clients and verifier for once
    clients = [helper.connect_to_host(), helper.connect_to_host(), helper.connect_to_host()]
    verifier = helper.connect_to_host()
    current_value = INITIAL_VAL

    # Seed initial value for the CAS key.
    try:
        verifier.put(CAS_KEY, str(INITIAL_VAL))
    except Exception as e:
        print(f"[cas_check] could not seed key: {e}", flush=True)
        return

    for round_idx in range(NUM_ROUNDS):
        target_value = current_value + 1

        #Barrier to synchronize the threads + main thread
        start_barrier = threading.Barrier(len(clients) + 1)

        # Results for the three threads.
        results = [None] * len(clients)
        # Threads for the three clients.
        threads = []
        for i in range(len(clients)):
            t = threading.Thread(
                target=attempt_cas,
                args=(clients[i], current_value, target_value, results, i, start_barrier),
            )
            threads.append(t)
            t.start()

        # Main thread barrier, releases all workers at once.
        start_barrier.wait(timeout=5)
        for t in threads:
            t.join()

        # Count the number of wins.
        wins = sum(1 for r in results if r is True)
        print(f"[cas_check] round={round_idx} CAS({current_value}->{target_value}) "
              f"t0={results[0]} t1={results[1]} t2={results[2]} wins={wins}", flush=True)

        # Atomicity: NEVER both succeed.
        # Zero wins is fine (all timed out under chaos).
        # Two wins means etcd committed two conflicting transactions! PANIC.
        always(
            wins <= 1,
            "At most one concurrent CAS writer succeeds (atomicity holds)",
            {
                "round": round_idx,
                "thread_0_won": results[0],
                "thread_1_won": results[1],
                "thread_2_won": results[2],
                "wins": wins,
                "current_value": current_value,
                "target_value": target_value,
                "timestamp": time.time(),
            },
        )

        # Read back the actual value and advance current_value for next round.
        try:
            response = verifier.get(CAS_KEY)
            current_value = int(response[0].decode('utf-8'))
        except Exception as e:
            print(f"Error verifying value: {e}")
            pass

if __name__ == "__main__":
    run_cas_check()


