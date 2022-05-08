#from io import TextIOWrapper
import os
from pickle import NONE
import time
import string
from datetime import date, datetime
import mysql.connector as connector


storing_data=connector.connect(host="localhost",database='ETL',user="root",passwd="Sravya@1528")
mycursor=storing_data.cursor()
1
QUERY ="SELECT * from directory"
mycursor.execute(QUERY)
databases=mycursor.fetchall()
path_for_directory_bool=False
file_path_bool=False


class Format:
    end = '\033[0m'
    underline = '\033[4m'

print("--------------------------------ETL----------------------------")
print( '''
                   {} Reader {}
    '''.format( Format.underline ,Format.end))
print('''
1) Show Directories
2) Input  Directory Path
3) Input File Path

''')
input_path=int(input("Select 1,2 or3: "))

def path_1():
    global path_for_directory,databases,path_for_directory_bool
    path_for_directory_bool=True
    for i in databases:
        
        print(i[0],":",i[1])
    file_input=int(input('Choose a serial no:  '))
    
    for i in databases:
        if (i[0]==file_input):
            path_for_directory=i[1]
        else:
            pass
    

if input_path==1:
    path_1()
    
elif  input_path==2 :
    path_for_directory_bool=True
    path_for_directory=input("Path:")
    path_for_directory=path_for_directory.replace("b'","")
    path_for_directory=path_for_directory.replace("'","")
    
    #path_for_directory=bytes(path_for_directory,'utf-8')
    #path_for_directory=path_for_directory.decode('utf-8')
elif input_path==3:
    file_path_bool=True
    file_path=input("file path: ")
    file_path=file_path.replace("b'","")
    file_path=file_path.replace("'","")
else:
    print("Wrong Entry, Please Try again")
    exit()



if path_for_directory_bool is True:
    try:
        os.chdir(path_for_directory)
    except :
        print("Serial No not Found")
        exit()


    if input_path==2:
        query="Insert into directory (data_path) value (%s)"
        mycursor.execute(query,(path_for_directory,))
        
elif file_path_bool is True:
    if input_path==3:
        query="Insert into file (data_path) value (%s);"
        mycursor.execute(query,(file_path,))
else:
    print("Wrong Entry, Please Try again")
    exit()

list_of_text_file=[]

def outputfile():
    global store_text
    date_time=datetime.now()
    now1=date_time.strftime(r"_%D_%H%M%S")
    now1=now1.replace("/","")
    file_name="output_file{}.txt".format(now1)
    data2=open(file_name,'w') 
    data2.writelines(store_text)
    data2.close()

    query="insert into output_directory (data_path) value (%s)"
    mycursor.execute(query,(file_name,))
    print("\n")
    print( '''
                   {} Writer {}
    '''.format( Format.underline ,Format.end))
    print("Output File Successfully created with path: {}".format(file_name))
    store_text.clear()
    storing_data.commit()
    exit()


#   APPEND PATH TO LIST
def adding_text_file(txt_file_path):
    list_of_text_file.append(txt_file_path)

inc=1
#  CHECKING TEXT FILES
if file_path_bool is False:
    print("\n")
    for file in os.listdir():
        if file.endswith(".txt"):
            txt_file_path=f"{path_for_directory}\\{file}"
            print(     inc," ",file     )
            inc=inc+1
            
        else:
            pass
    file_name=input("Enter File name ")
    file_name=file_name.replace(".txt","")
    for file in os.listdir():
        if file.endswith("{}.txt".format(file_name)):
            txt_file_path=f"{path_for_directory}\\{file}" 
            list_of_text_file.append(txt_file_path)
        
else:
    list_of_text_file.append(file_path +'.txt')

store_text=[]
def capitalizing():
    global data,store_text
    store_text=[]
    for line in data:
        line=line.title()
        print(line)
        store_text.append(line)
    outputfile()

dictonary_occurance_words=dict()


def counting_words():
    global store_data,dictonary_occurance_words,data,store_text
    
   
    for line in data:
        line=line.strip()
        line=line.lower()
        line =line.translate(line.maketrans("", "", string.punctuation))
        storing_words=line.split(" ")
        for word in storing_words:
            if word in dictonary_occurance_words:
                dictonary_occurance_words[word]=dictonary_occurance_words[word]+1  
            else:
                dictonary_occurance_words[word]=1

    for key in list(dictonary_occurance_words.keys()):
        print( "{}:{}".format(key,dictonary_occurance_words[key]))
        store_text.append( "{}:{}".format(key,dictonary_occurance_words[key]))
    outputfile()



for path in list_of_text_file:
    
    path=path.replace("b'","")
    path=path.replace("'","")
    
    path=bytes(path,'utf-8')
    path=path.decode('utf-8')
    try:
        data1=open(path,'r') 
    except:
        print("Path incorrect")
        exit()
    data=data1.readlines()
    store_data=data1.read()

    print( '''
                   {} Transformation {}
    1)Captalize
    2)Read all unique words from the file 
    '''.format( Format.underline ,Format.end))

    modify_input=int(input("Choose a Number: "))
    if 1==modify_input:
        print(Format.underline + " STORED TEXT :" + Format.end)
        capitalizing()
        print("\n")
    elif modify_input==2:
        print(Format.underline + " NO OF WORDS :" + Format.end)
        counting_words()
        print("\n")
    else:
        print("Wrong Entry, Please Try again")
        exit()
    data1.close()
    print("\n")
    dictonary_occurance_words.clear()



storing_data.commit()