import threading

# Shared resource (simulated lock and process ID)
lock = 0
process_id = 1  # This will be used as the identifier in compare-and-swap

# Compare-and-Swap function
def compare_and_swap(memory, expected, new_value):
    global lock
    if memory == expected:
        lock = new_value
        return True
    return False

# Function that simulates a process trying to acquire the lock
def process(name, id):
    while True:
        # Try to acquire the lock
        if compare_and_swap(lock, 0, id):
            print(f"{name} acquired the lock.")
            # Critical section
            break
        else:
            print(f"{name} failed to acquire the lock. Retrying...")
    
    # Simulate some work
    import time
    time.sleep(1)
    
    # Release the lock
    global lock
    lock = 0
    print(f"{name} released the lock.")

# Create two threads simulating two processes
thread1 = threading.Thread(target=process, args=("Process 1", 1))
thread2 = threading.Thread(target=process, args=("Process 2", 2))

# Start the threads
thread1.start()
thread2.start()

# Wait for both threads to finish
thread1.join()
thread2.join()

print("Both processes have completed.")
