import yaml
import time
import logging
import datetime
from yaml.loader import SafeLoader


class Task():
    def __init__(self):
        pass

    def TimeFunction(self, fnc_input, execution_time, task_name, execution_type, string):
        if(execution_type == 'Sequential'):
            with open('logfile.txt', 'a', newline='') as file:
                # x = str(datetime.datetime.now()) + string
                file.write(str(datetime.datetime.now()) + string + " Entry\n")
                time.sleep(int(execution_time))
                file.write(str(datetime.datetime.now()) + string +
                           " Executing TimeFunction({}, {})".format(fnc_input, execution_time) + "\n")
                file.write(str(datetime.datetime.now()) + string + " Exit\n")
                file.close()
                # file.write(datetime.datetime.now() + ";M1A_Workflow Exit")
        else:


class workflow_config(Task):

    def __init__(self, dict_file):
        self.dict_file = dict_file
        # self.task_name = task_name
        # self.execution_type = execution_type
        # self.string = string

    def parsing_dict(self, activities, task_name, execution_type, string):
        self.dict_file = activities
        print(task_name)
        if self.dict_file["Type"] == "Flow":
            with open('logfile.txt', 'a', newline='') as file:
                file.write(str(datetime.datetime.now()) + string + " Entry\n")
                file.close()
            exec_type = self.dict_file["Execution"]
            for t_name, values in self.dict_file["Activities"].items():
                self.parsing_dict(values, t_name, exec_type, string+"."+t_name)
            with open('logfile.txt', 'a', newline='') as file:
                file.write(str(datetime.datetime.now())
                           + string + " Exit\n")
                file.close()
        else:
            # task_name = dict_file[]
            inputs = self.dict_file["Inputs"]
            fnc_input = inputs["FunctionInput"]
            execution_time = inputs["ExecutionTime"]
            self.TimeFunction(fnc_input, execution_time,
                              task_name, execution_type, string)


def main():
    stream = open("Milestone1A.yaml", 'r')
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
        dict1A['M1A_Workflow'], "M1A_Workflow", None, ";M1A_Workflow")
    # with open('logfile.txt', 'a', newline='') as file:
    #     file.write(str(datetime.datetime.now())+";Milestone1A Exit\n")
    #     file.close()


if __name__ == "__main__":
    main()
