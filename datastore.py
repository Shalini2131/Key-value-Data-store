import time
import threading
from threading import *

data_dict=dict()#data store dictionary


#Creating key_value pair with specific timeout(default is 0)
def create(key,val,timeout=0):
	if key in data_dict:
		print("Error!! Key already exists")
	else:
		if(key.isalpha()):
			if((len(data_dict)<(1024*1024*1024)) and val<=(16*1024*1024)):
				if timeout==0:
					li=[val,timeout]
				else:
					li=[val,time.time()+timeout]
				if len(key)<33:
					data_dict[key]=li
			else:
				print("Error! Memory limit exceeded..")
		else:
			print("Invalid key name!")


#Reading the value using key
def read(key):
    if key not in data_dict:
        print("error: given key does not exist in database. Please enter a valid key")
    else:
        temp=data_dict[key]
        if temp[1]!=0:
            if time.time()<temp[1]: 
                string=str(key)+":"+str(temp[0]) 
                return string
            else:
                print("error: time-to-live expired")
        else:
            string=str(key)+":"+str(temp[0])
            return string

#Deleting specific key-value
def delete(key):
    if key not in data_dict:
        print("error!! key does not exist,enter a valid key")
    else:
        temp=data_dict[key]
        if temp[1]!=0:
            if time.time()<temp[1]:
                del data_dict[key]
                print("key is deleted successfully!")
            else:
                print("Error!Time-to-live expired")
        else:
            del data_dict[key]
            print("key is deleted successfully!")

if __name__ == "__main__":
    create("hello", 10)#with default timeout as 0
    create("sample", 30,2500)#with specified timeout 

    read("hello")#reading key
    read("sample")

    create("sample", 26)#creating existing key(dives error)

    delete("sample")#deleting using key name
    delete("hello")

    create("hello", 21)#again creating new key with value

    #to demonstrate that the data store is thread safe by creating n threads(here 2)
    thread1=threading.Thread(target=(create),args=("hai",22,3600))
    thread1.start()

    thread2=threading.Thread(target=(read),args=("hai",))
    thread2.start()

    #thread3=threading.Thread(target=(delete),args=("hai"))
    #thread3.start()
