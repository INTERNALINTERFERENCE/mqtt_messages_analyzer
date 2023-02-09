from apscheduler.schedulers.blocking import BlockingScheduler
from threading import Thread
import client

def write_to_file_analyzed_data():
    lines = ['{};{}'.format(k, ';'.join(map(str, v))) for k, v in client.dict.items()]
    analyzed_result = '\n'.join(lines)
    
    with open('analyzed.csv', 'w') as out:
        out.write(analyzed_result)
  
def start_scheduler():
    scheduler = BlockingScheduler()
    scheduler.add_job(write_to_file_analyzed_data, 'interval', minutes=1)
    scheduler.start()

if __name__ == "__main__":
    client.read_scv()
    thread1 = Thread(target=client.create_mqtt_client)
    thread2 = Thread(target=start_scheduler)

    thread1.start()
    thread2.start()
    thread1.join()
    thread2.join()
    
