import threading
import os

def start_bill_generator():
    """Runs the bill generator script."""
    os.system("python RealtimeBillGenerator.py")

def start_bill_processor():
    """Runs the bill processor script."""
    os.system("python proj_bill_processor_realtime_logger.py")

if __name__ == "__main__":
    t1 = threading.Thread(target=start_bill_generator)
    t2 = threading.Thread(target=start_bill_processor)

    t1.start()
    t2.start()

    t1.join()
    t2.join()
