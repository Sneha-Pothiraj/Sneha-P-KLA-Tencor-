import yaml
import time
import logging
import datetime
import threading
from yaml.loader import SafeLoader

class Task():
    def __init__(self):
        pass

    def TimeFunction(self, fnc_input, execution_time, task_name, execution_type, string):
        # if(execution_type == 'Sequential'):
        with open('logfile.txt', 'a', newline='') as file:
                # x = str(datetime.datetime.now()) + string
            file.write(str(datetime.datetime.now()) + string + " Entry\n")
            time.sleep(int(execution_time))
            file.write(str(datetime.datetime.now()) + string +
                        " Executing TimeFunction({}, {})".format(fnc_input, execution_time) + "\n")
            file.write(str(datetime.datetime.now()) + string + " Exit\n")
            file.close()
                # file.write(datetime.datetime.now() + ";M1A_Workflow Exit")
        # else:
        #     with open('logfile.txt', 'a', newline='') as file:
        #         for i in range(num):
        #             file.write(str(datetime.datetime.now()) +
        #                        string + " Entry\n")
        #             time.sleep(int(execution_time))
        #             file.write(str(datetime.datetime.now()) + string +
        #                    " Executing TimeFunction({}, {})".format(fnc_input, execution_time) + "\n")
        #             file.write(str(datetime.datetime.now()) + string + " Exit\n")
        #             file.close()

            
class workflow_config(Task):

    def __init__(self, dict_file):
        self.dict_file=dict_file
        # self.task_name = task_name
        # self.execution_type = execution_type
        # self.string = string
    def parsing_dict(self, activities, task_name, execution_type, string):
        #self.dict_file = activities
        #print(task_name)
        if activities["Type"] == "Flow":
            # print("***"+activities["Type"])
            with open('logfile.txt', 'a', newline='') as file:
                file.write(str(datetime.datetime.now()) + string + " Entry\n")
                file.close()
            # print("####: \n", activities)
            exec_type = activities["Execution"]
            # print("Exec: \n", self.dict_file)
            key_list = []
            thread_list = []
            if exec_type == "Concurrent":
                for t_name, values in activities["Activities"].items():
                    print(values)
                    key_list.append((t_name, values))
                #    for t, v in ["Activities"].items():
                #         key_list.append(values[t])

                for tn, values in key_list:
                    if values["Type"] == "Task":
                        inputs = values["Inputs"]
                        fnc_input = inputs["FunctionInput"]
                        execution_time = inputs["ExecutionTime"]
                        t = threading.Thread(target=self.TimeFunction, args=(
                            fnc_input, execution_time, tn, None, string + "."+tn))
                            # thread_list.append(t)
                        t.start()
                    else:
                        # with open('logfile.txt', 'a', newline='') as file:
                        #     file.write(str(datetime.datetime.now()) + string + " Entry\n")
                        #     file.close()
                        t = threading.Thread(target = self.parsing_dict, args=(values, tn, exec_type, string+"."+tn))
                        t.start()
                        # with open('logfile.txt', 'a', newline='') as file:
                        #     file.write(str(datetime.datetime.now()) + string + " Exit\n")
                        #     file.close()
                    thread_list.append(t)
                        
                        # values, t_name, exec_type, string+"."+t_name
                        # for x in range(len(thread_list)):
                        #     thread_list[x].start()
                        # for x in range(len(thread_list)):
                        #     thread_list[x].join() 
                for x in range(len(thread_list)):
                    thread_list[x].join()
            else:
                for t_name, values in activities["Activities"].items():
                    # with open('logfile.txt', 'a', newline='') as file:
                    #     file.write(str(datetime.datetime.now()) + string + " Entry\n")
                    #     file.close()
                    self.parsing_dict(values, t_name, exec_type, string+"."+t_name)

                    # with open('logfile.txt', 'a', newline='') as file:
                    #     file.write(str(datetime.datetime.now())   + string + " Exit\n")
                    #     file.close()

                # num = len(values.keys())
                # for i in range(num):
                #     t = threading.Thread(target=self.TimeFunction, args=(
                #         fnc_input, execution_time, task_name, execution_type, string, num))
                #     thread_list.append(t)
            with open('logfile.txt', 'a', newline='') as file:
                file.write(str(datetime.datetime.now()) 
                            + string + " Exit\n")
                file.close()
        else:
            # task_name = dict_file[]
            inputs = activities["Inputs"]
            fnc_input = inputs["FunctionInput"]
            execution_time = inputs["ExecutionTime"]
            self.TimeFunction(fnc_input, execution_time, task_name, execution_type, string)

def main():
    stream = open("Milestone1B.yaml", 'r')
    dict1A = yaml.load(stream, Loader=SafeLoader)
    for key, value in dict1A.items():
        print(key + " : " + str(value))
    # activity_list = []
    # for i in range(len(dictionary1A)):
        # print(dictionary1A[i])
    # with open('logfile.txt', 'a', newline='') as file:
    #     file.write(str(datetime.datetime.now()) + ";Milestone1A Entry\n")
    #     file.close()
    work_flow = workflow_config(dict1A)
    work_flow.parsing_dict(
        dict1A['M1B_Workflow'], "M1B_Workflow", None, ";M1B_Workflow")
    # with open('logfile.txt', 'a', newline='') as file:
    #     file.write(str(datetime.datetime.now())+";Milestone1A Exit\n")
    #     file.close()

if __name__ == "__main__":
    main()
