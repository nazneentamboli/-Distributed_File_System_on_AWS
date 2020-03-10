from flask import Flask
import base64
import requests, sys
import os
import boto3
from flask_restful import Resource, Api

app = Flask(__name__)
api = Api(app)
name_node_address = "34.212.21.208:5000"
CHUNK_SIZE = 2<<27

def createFile():
    #we could user environ variables or input for access key and secret key
    s3 = boto3.client('s3',
                      aws_access_key_id='AKIA2FCNCD7QPLALQFW5',
                      aws_secret_access_key='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')
    BucketName = input("enter bucketname: ")
    readFrom = input("enter filepath to read from: ")
    nameOfNewFile = input("enter name of new file: ")
    s3.download_file(BucketName, readFrom, nameOfNewFile)
    size = os.stat(nameOfNewFile)
    fileSize = size.st_size
    filepath = input("please enter filepath: ")

    # fileSize = os.stat(filepath).st_size #1048577 #hardcoded for now, will be actual filesize later

    listofnodes = [] # List of datanodes for a certain block_id

    r = requests.post('http://' + name_node_address + '/createFile/', data={"file_path": filepath,
                                                                                 "fileSize": fileSize})
    if r.status_code == 200:
        print("Okay to upload")
        write_response = r.json()
        file = open(nameOfNewFile, 'rb')
        sizeCounter = fileSize

        for blockid in write_response:
            if sizeCounter > CHUNK_SIZE:
                blockData = file.read(CHUNK_SIZE)
            else:
                blockData = file.read()
            listofnodes = write_response[blockid]
            request_node = listofnodes[0]
            send(blockid, request_node, blockData)
            #replica pipeline to other datanodes in the list
            send_replica(blockid, listofnodes, request_node)
            sizeCounter = sizeCounter - CHUNK_SIZE
        file.close()
    elif r.status_code == 409:
        print("File Name Already Exists.")
        #sys.exit(1)
    elif r.status_code == 406:
        print("Ran out of storage space.")
    else:
        print("ERROR " + str(r.status_code) + " : " + str(r.reason))

#Send the given block to the first selected datanode in the list
def send(blockID, ip, blockData):
    data = str(base64.b64encode(blockData))
    data = data[2: len(data) - 1]

    task = {"data": data, "size": blockData.__sizeof__()}
    r = requests.post('http://' + ip + ':5000/BlockData/' + blockID, json=task)
    # r = requests.post('http://' + ip + '/BlockData/' + blockID, json=task)
    if r.status_code != 200:
        print("Error code: " + str(r.status_code))
    else:
        print(blockID + " written to " + ip)

#Pipeline and send replicas to other datanodes in rr fashion
def send_replica(blockid, listofnodes, request_node):
    for i in range(len(listofnodes) - 1):
        request_node = listofnodes[i]
        target_node = listofnodes[i + 1]
        task = {"target_node": target_node, "block_id": blockid}
        r = requests.post("http://" + request_node + ":5000/SendCopy", json=task)
        if r.status_code != 200:
            print(str(r.status_code))
            print("Block_id" + blockid + "copied to" + target_node)

# delete the file with given filepath
def deleteFile():
    filepath = input("please enter filepath to delete: ")
    r = requests.delete('http://' + name_node_address + '/deleteFile', data={'file_path': filepath})
    if r.status_code == 200:
        print(filepath + " deletion completed.")
    elif r.status_code == 404:
        print("file path invalid.")
    else:
        print("Delete failed for - " + filepath)

# returns the subdirectories and files with given filepath
def ls():
    path = input("please enter the path: ")
    r = requests.get('http://' + name_node_address + '/listAll', data={'file_path': path})
    print(r.json())

def readFile():
    print ('read')
    filepath = input('Enter filepath: ')
    r = requests.get('http://' + name_node_address + '/getFileBlocks/' + filepath)
    block_list = r.json()
    with open("New" + filepath, "wb") as f:
        for block_id in block_list:
            nodelist = block_list[block_id]
            datanode = nodelist[0]
            resp = requests.get("http://" + datanode + ":5000/BlockData/" + block_id)
            blockdata = resp.json()
            f.write(base64.b64decode(blockdata["data"]))
    f.close()

def getBlockInfo():
    fileName = input("please enter the file name: ")
    r = requests.get('http://' + name_node_address  + '/list', data={'file_name': fileName})
    if r.status_code == 200:
        print(r.json())
    else:
        print("List failed for - " + fileName)

#commands['createFile'] = createFile()
def makeDirectory():
    path = input("enter path: ")
    r = requests.post('http://' + name_node_address + '/directories', data={'file_path': path})
    if r.status_code == 409:
        print("name already exists")
    elif r.status_code != 200:
        print("Error code: " + str(r.status_code))


# remove directory if it's empty
# error if directory is not empty, or DNE
def removeDirectory():
    path = input("enter path: ")
    r = requests.delete('http://' + name_node_address + '/removeDirectory', data={'file_path': path})
    print(r.json())

def main():

    while True:
        txt = input("Welcome to SUFS. Enter write, read, delete, list, ls, mkdir, or rmdir: ")
        txt = txt.lower()
        if txt == "write":
            createFile()
        elif txt == "delete":
            deleteFile()
        elif txt == "read":
            readFile()
        elif txt == "ls":
            ls()
        elif txt == "list":
            getBlockInfo()
        elif txt == "mkdir":
            makeDirectory()
        elif txt == "rmdir":
            removeDirectory()
        elif txt == "quit":
            sys.exit(0)
        else:
            print ("invalid command")




if __name__ == '__main__':
    main()
    
app.debug = True # debug mode on
app.run()
