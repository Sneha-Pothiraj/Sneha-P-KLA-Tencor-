import yaml
import time
import logging
import datetime
import threading
import pandas as pd
import re

from yaml.loader import SafeLoader

# dataload_map = {}

class Task():
    dataload_map = {}
    # self.dataload_map=0
    def __init__(self):
        pass

    def TimeFunction(self, fnc_input, execution_time, task_name, execution_type, string):
        # if(execution_type == 'Sequential'):
        with open('logfile2A.txt', 'a', newline='') as file:
            # x = str(datetime.datetime.now()) + string
            file.write(str(datetime.datetime.now()) + string + " Entry\n")
            time.sleep(int(execution_time))
            file.write(str(datetime.datetime.now()) + string +
                       " Executing TimeFunction({}, {})".format(fnc_input, execution_time) + "\n")
            file.write(str(datetime.datetime.now()) + string + " Exit\n")
            file.close()

    def DataLoad(self, fnc_input, task_name, execution_type, string):
        csv = fnc_input
        df = pd.read_csv(csv)
        # dataload_map[string + ".NoOfDefects"] =
        self.dataload_map[string+"NoOfDefects"] = len(list(df))
        # return [csv, len(list(df))]

class workflow_config(Task):

    def __init__(self, dict_file):
        self.dict_file = dict_file

    def parsing_dict(self, activities, task_name, execution_type, string):
        
        if activities["Type"] == "Flow":
            with open('logfile2A.txt', 'a', newline='') as file:
                file.write(str(datetime.datetime.now()) + string + " Entry\n")
                file.close()
            exec_type = activities["Execution"]
            # print("Exec: \n", self.dict_file)
            key_list = []
            thread_list = []
            if exec_type == "Concurrent":
                for t_name, values in activities["Activities"].items():
                    print(values)
                    key_list.append((t_name, values))

                for tn, values in key_list:
                    if values["Type"] == "Task":
                        fnc_name = values["Function"]
                        if(fnc_name == "TimeFunction"):
                            inputs = values["Inputs"]
                            fnc_input = inputs["FunctionInput"]
                            execution_time = inputs["ExecutionTime"]
                            t = threading.Thread(target=self.TimeFunction, args=(
                            fnc_input, execution_time, tn, None, string + "."+tn))
                        else:
                            inputs = values["Inputs"]
                            fnc_input = inputs["Filename"]
                            if not (values[task_name]["Condition"]):
                                t = threading.Thread(target=self.DataLoad, args=(
                                fnc_input, task_name, execution_type, string + "." + tn))
                            else:
                                condition = values[task_name]["Condition"]
                                r1 = condition.split("(")[1].split(")")
                                key_v = r1[0]
                                cond = r1[1].split()
                                symbol = cond[0]
                                number = cond[1]
                                if(key_v in self.dataload_map.keys()):
                                    x = f"self.dataload_map['{key_v}']{symbol}{number}"
                                    bool = eval(x)
                                    if(bool):
                                        self.DataLoad(fnc_input, task_name,
                                          execution_type, string)
                                    else:
                                        with open('logfile2A.txt', 'a', newline='') as file:
                                            file.write(
                                    str(datetime.datetime.now()) + string + "Skipped\n")
                                            file.write(
                                    str(datetime.datetime.now()) + string + "Exit\n")
                                    file.close()
                                
                        # thread_list.append(t)
                        t.start()
                    else:
                        t = threading.Thread(target=self.parsing_dict, args=(
                            values, tn, exec_type, string+"."+tn))
                        t.start()
                    thread_list.append(t)

                for x in range(len(thread_list)):
                    thread_list[x].join()
            else:
                for t_name, values in activities["Activities"].items():
                    self.parsing_dict(
                        values, t_name, exec_type, string+"."+t_name)

            with open('logfile2A.txt', 'a', newline='') as file:
                file.write(str(datetime.datetime.now())
                           + string + " Exit\n")
                file.close()
        else:
            # task_name = dict_file[]
            fnc_name = activities["Function"]
            if(fnc_name == "TimeFunction"):
                inputs = activities["Inputs"]
                fnc_input = inputs["FunctionInput"]
                execution_time = inputs["ExecutionTime"]
                self.TimeFunction(fnc_input, execution_time, task_name, execution_type, string)
            
            else:
                inputs = activities["Inputs"]
                fnc_input = inputs["Filename"]
                print(activities[task_name].keys())
                if "Condition" in activities[task_name].keys():
                    self.DataLoad(fnc_input, task_name, execution_type, string)
                else:
                    condition = activities[task_name]["Condition"]
                    r1 = condition.split("(")[1].split(")")
                    key_v = r1[0]
                    cond = r1[1].split()
                    symbol = cond[0]
                    number = cond[1]
                    if(key_v in self.dataload_map.keys()):
                        x = f"self.dataload_map['{key_v}']{symbol}{number}"
                        bool = eval(x)
                        if(bool):
                            self.DataLoad(fnc_input, task_name,
                                          execution_type, string)
                        else:
                            with open('logfile2A.txt', 'a', newline='') as file:
                                file.write(str(datetime.datetime.now()) + string + "Skipped\n")
                                file.write(
                                    str(datetime.datetime.now()) + string + "Exit\n")
                                file.close()
                # data_load_map[]


def main():
    stream = open("Milestone2A.yaml", 'r')
    dict1A = yaml.load(stream, Loader=SafeLoader)
    for key, value in dict1A.items():
        print(key + " : " + str(value))
    work_flow = workflow_config(dict1A)
    work_flow.parsing_dict(
        dict1A['M2A_Workflow'], "M2A_Workflow", None, ";M2A_Workflow")

if __name__ == "__main__":
    main()
