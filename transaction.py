import threading
import time
import random

# Shared resource for simulation
shared_resource = {"data": 0}
lock = threading.Lock()

class TransactionState:
    def __init__(self):
        self.active = True
        self.rollback_needed = False

class Transaction:
    def __init__(self, transaction_id):
        self.transaction_id = transaction_id
        self.state = TransactionState()

    def execute_query(self, query_number):
        print(f"Transaction {self.transaction_id} executing query {query_number}.")
        # Simulate query execution
        time.sleep(random.uniform(0.1, 0.5))  # Simulate some execution time

        # Randomly simulate a failure in the query execution
        if random.random() < 0.3:  # 30% chance of query failure
            print(f"Transaction {self.transaction_id} encountered failure in query {query_number}.")
            self.state.rollback_needed = True
            raise Exception(f"Failure in query {query_number}.")

    def partial_rollback(self):
        print(f"Transaction {self.transaction_id} performing partial rollback.")
        time.sleep(1)  # Simulate partial rollback time
        print(f"Transaction {self.transaction_id} partial rollback completed.")

    def full_rollback(self):
        print(f"Transaction {self.transaction_id} rolling back.")
        time.sleep(1)  # Simulate rollback time
        print(f"Transaction {self.transaction_id} rolled back.")

    def commit(self):
        print(f"Transaction {self.transaction_id} committing.")
        time.sleep(1)  # Simulate commit time
        print(f"Transaction {self.transaction_id} committed successfully.")

    def run(self):
        print(f"Transaction {self.transaction_id} started.")
        
        try:
            # Execute queries
            with lock:
                self.execute_query(1)
                self.execute_query(2)

                # Simulate timeout or deadlock scenario
                if random.random() < 0.2:  # 20% chance of timeout or deadlock
                    print(f"Transaction {self.transaction_id} encountered a timeout or deadlock.")
                    self.state.active = False
                    raise Exception("Timeout or deadlock occurred.")

            # Check if all queries were successful
            if not self.state.rollback_needed:
                self.commit()
            else:
                # Handle partial rollback if needed
                if random.random() < 0.5:  # 50% chance of partial rollback if needed
                    self.partial_rollback()
                self.full_rollback()
        except Exception as e:
            # Handle exceptions, including partial rollback
            if self.state.rollback_needed:
                self.full_rollback()
            else:
                print(f"Transaction {self.transaction_id} aborted: {str(e)}")

def start_transactions():
    threads = []
    for i in range(3):  # Simulate 3 concurrent transactions
        transaction = Transaction(transaction_id=i+1)
        thread = threading.Thread(target=transaction.run)
        threads.append(thread)
        thread.start()
    
    # Wait for all transactions to complete
    for thread in threads:
        thread.join()

    print("All transactions have completed.")

# Run the simulation
start_transactions()
