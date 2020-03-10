import base64
from flask import Flask, request
from flask_restful import Resource, Api, abort
import datetime
import time
import sys
import threading
import requests
import os
import tempfile
import shutil
import socket
from ec2_metadata import ec2_metadata
from http import HTTPStatus
import boto3

app = Flask(__name__)
api = Api(app)
name_node_address = "34.212.21.208:5000"
DataNodeID = ""
temp_folder = "./temp"
data_folder = "./data_blocks"
block_list = {}
list_of_blocks = []


# store the data block in a temp folder and then
# move to the permanent data_folder storage
# file_name: desired file name to store the data block under the data_folder
def storeFileBlock(file_name, data_block):
    temp = tempfile.NamedTemporaryFile(dir=temp_folder, mode='w+b', delete=False)
    temp.write(data_block)
    if file_name not in list_of_blocks:
        list_of_blocks.append(file_name)
    try:
        # print ("temp:" + str(temp))
        # print ("temp.name:" + str(temp.name))
        dest = shutil.move(os.path.join(temp_folder, temp.name), os.path.join(data_folder, file_name))
    finally:
        temp.close()


# return the fileblock under the data_folder
def getFileBlock(file_name):
    f = open(os.path.join(data_folder, file_name), 'r', 1)
    data = f.read()
    f.close()
    return data

class BlockData(Resource):

    # store data block and use block id as the file name
    # return bad request if missing any info
    def post(self, block_id):
        print("Storing..." + block_id)
        response = request.get_json()

        if "size" not in response or "data" not in response or type(response["size"]) != int:
            return HTTPStatus.BadRequest
        size = response["size"]
        block_list[block_id] = size
        list_of_blocks.append(block_id)
        storeFileBlock(block_id, response["data"].encode())
        print("Successful Storing: " + block_id)
        return HTTPStatus.OK

    # return data block with a given block id
    # return 404 if the block ID is not found
    def get(self, block_id):
        print("Reading..." + block_id)
        if block_id not in block_list:
            return HTTPStatus.NotFound
        data = getFileBlock(block_id)
        print("Successful Return " + block_id)
        return {"data": data}, HTTPStatus.OK


api.add_resource(BlockData, "/BlockData/<string:block_id>")


class SendCopy(Resource):
    # handle data replication request from namenode
    # send copy from the datanode to target datanode
    def post(self):
        response = request.get_json()
        print(response)
        if "block_id" not in response or "target_node" not in response:
            return HTTPStatus.BadRequest
        target_node = response["target_node"]
        block_id = response["block_id"]
        data = getFileBlock(block_id)
        size = block_list.get(block_id)
        task = {"data": data, "size": size}
        r = requests.post("http://" + target_node + ":5000/BlockData/" + block_id, json=task)
        if r.status_code != 200:
            print(str(r.status_code))
        else:
            print("Successfully sent copy.")
            return HTTPStatus.OK


api.add_resource(SendCopy, "/SendCopy")


# delete block with the blcok_id provided from NameNode
# sample call on POSTMAN: DELETE http://localhost:5001/deleteBlock    BODY: {"Target":"test"}
class deleteBlock(Resource):
    def delete(self):
        r = request.get_json()
        block = r["Target"]
        path = os.path.join(data_folder, block)
        # dest = (data_folder + block)
        print(path)
        try:
            os.remove(path)
            list_of_blocks.remove(block)
        except OSError as e:  # name the Exception `e`
            print("Failed with:", e.strerror)
            print("Error code:", e.code)
        return ("Target file block removed.")


api.add_resource(deleteBlock, '/deleteBlock')


def get_Host_name_IP():
    try:
        global DataNodeID
        DataNodeID = ec2_metadata.public_ipv4
        print("IP is: ", DataNodeID)
    except:
        print("Unable to get Hostname and IP")


def sendDataNodeInfo():
    starttime = time.time()
    while True:
        print("datanode is is: ", DataNodeID)
        payload = {'DataNodeID': DataNodeID, 'list_of_blocks': list_of_blocks}
        r = requests.post('http://' + name_node_address + '/getDataNodeInfo',
                          json=payload)
        if r.status_code != 200:
            print('error: ', r.status_code)
        time.sleep(30.0 - ((time.time() - starttime) % 30.0))


def main():
    get_Host_name_IP()
    sendDataNodeInfo()


if __name__ == '__main__':

    get_Host_name_IP()
    t = threading.Thread(target=sendDataNodeInfo)
    t.start()
    app.run(host=os.getenv('LISTEN', '0.0.0.0'))

