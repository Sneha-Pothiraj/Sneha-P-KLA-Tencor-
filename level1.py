import yaml
import time
import logging
import datetime
from yaml.loader import SafeLoader

class recursion():
    def __init__(self):
        pass

    def rec_call(self):

    def TimeFunction(input, execution_time, task_name, execution_type):
        if(execution_type == 'Sequential'):
            with open('logfile.txt', 'w', newline='') as file:
            file.write(datetime.datetime.now() + ";M1A_Workflow Entry")
            time.sleep(execution_time)
            file.write(datetime.datetime.now() + ";M1A_Workflow."+ task_name + "TimeFunction({})".format(execution_time))
            file.write(datetime.datetime.now() + ";M1A_Workflow Exit")
        else:


class workflow_config(recursion):

    def __init__(self, dict_file):
        self.dict_file=dict_file
    
    def parsing_dict():
        for key, value in dict_file.items():
            if value == 'Flow':
                # if execution == Sequential:


                    
    def result():


def main():
    stream = open("Milestone1A.yaml", 'r')
    dict1A = yaml.load(stream, Loader=SafeLoader)
    for key, value in dict1A.items():
        print(key + " : " + str(value))
    activity_list = []
    # for i in range(len(dictionary1A)):
        # print(dictionary1A[i])
    work_flow = workflow_config(dict1A)
    work_flow.parsing_dict()
    


        

    

if __name__ == "__main__":
    main()
