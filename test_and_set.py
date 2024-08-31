import threading

# Shared resource (simulated lock)
lock = 0

# Test-and-Set function
def test_and_set():
    global lock
    old_value = lock
    lock = 1
    return old_value

# Function that simulates a process trying to acquire the lock
def process(name):
    while True:
        if test_and_set() == 0:
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
thread1 = threading.Thread(target=process, args=("Process 1",))
thread2 = threading.Thread(target=process, args=("Process 2",))

# Start the threads
thread1.start()
thread2.start()

# Wait for both threads to finish
thread1.join()
thread2.join()

print("Both processes have completed.")
