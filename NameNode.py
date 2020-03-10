import requests
from flask import Flask, request
from flask_restful import Resource, Api, abort
import werkzeug.exceptions as HTTPStatus
import os.path
import time
import json
import threading

app = Flask(__name__)
api = Api(app)

files = {}
# files = {'amazon_reviews_us_Electronics_v1_00.tsv.gz' : { 'block-0': ['Node1', 'Node2'],
#                                                           'block-1': ['Node2']
#                                                         },
#          'amazon_reviews_us_Electronics_v2_00.tsv.gz' : { 'block-0': ['Node1', 'Node2'],
#                                                           'block-1': ['Node2']
#                                                         }
# }

blockNumber = 1  # keeps track of the current block number
blockSize = 128000000  # 128MB
# will probably replace this/add a dictionary with ip address as key or value
dataNodeNum = 0  # keeps track which datanode to assign block to
numberOfNodes = 0  # random hardcoded value, will reflect actual number of datanodes later
replicationFactor = 3
heartbeat_Timeout = 61
recentDataNodes = {}
nodesAndBlocks = {}
blockReportInfo = {}  # key is node, value is blocks


# Returns a list of active data nodes
def getActiveDataNodes():
    output = []
    current_time = time.time()
    for node_name, time_seen in recentDataNodes.items():
        time_diff = current_time - time_seen
        if time_diff < heartbeat_Timeout:
            output.append(node_name)
    return output


def checkNumReplicas():
    global nodesAndBlocks
    for block in nodesAndBlocks:
        intBlock = int(block)
        curr = nodesAndBlocks[intBlock]
        length = len(curr)
        if length < replicationFactor:
            activeNodes = getActiveDataNodes()
            replicationCandidates = set(activeNodes) - set(curr)
            replicationCandidates = list(replicationCandidates)
            for i in range(replicationFactor - length):
                print('candidates: ', replicationCandidates)
                request_node = curr[0]
                print('requestnode: ', request_node)
                target_node = replicationCandidates[i]
                print('target_node: ', target_node)
                strBlock = str(block)
                task = {'target_node': target_node, 'block_id': strBlock}
                r = requests.post("http://" + request_node + ":5000/SendCopy", json=task)
                if r.status_code != 200:
                    print(str(r.status_code))
                else:
                    print("Block_id" + strBlock + "copied to" + target_node)


def handleNodeFailure():
    starttime = time.time()
    global nodesAndBlocks
    while True:
        activeNodes = getActiveDataNodes()
        print('active nodes are: ', activeNodes)
        allNodes = blockReportInfo.keys()
        failedNodes = set(allNodes) - set(activeNodes)
        failedNodes = list(failedNodes)
        print("failed nodes: ", failedNodes)
        for nodes in failedNodes:
            failedNodeBlocks = blockReportInfo[nodes]
            print('failed node blocks: ', failedNodeBlocks)
            del blockReportInfo[nodes]
            for block in failedNodeBlocks:
                intBlockNum = int(block)
                listOfBlocks = []
                if intBlockNum in nodesAndBlocks:
                    listOfBlocks = nodesAndBlocks[intBlockNum]
                if nodes in listOfBlocks:
                    listOfBlocks.remove(nodes)
        checkNumReplicas()
        time.sleep(15)


# receives file size from client, divides that by 128MB to get num blocks
# assigns blocks and replicas to data nodes in round robin fashion
# returns to client as dictionary with key as block id and value as list of datanodes
# will probably replace datanode id here with ip address of datanode
# there are some print statements for debugging
class CreateFile(Resource):
    def post(self):
        global dataNodeNum
        global blockNumber
        global nodesAndBlocks
        filePath = request.form['file_path']
        numBlocks = 0
        # if filePath in files or os.path.exists(filePath):
        #     return None, 409
        # f = open(filePath, "w+")

        files[filePath] = {}
        fileSize = request.form['fileSize']
        fileSize = int(fileSize)
        activeDataNodes = getActiveDataNodes()
        numberOfNodes = len(activeDataNodes)

        if fileSize > blockSize:
            numBlocks = fileSize // blockSize

        if (fileSize % blockSize) > 0:
            numBlocks += 1

        for x in range(numBlocks):
            for y in range(replicationFactor):
                dataNodeNum = dataNodeNum % numberOfNodes
                if blockNumber not in nodesAndBlocks:
                    nodesAndBlocks[blockNumber] = [str(activeDataNodes[dataNodeNum])]
                    dataNodeNum += 1
                else:
                    listOfDataNodes = nodesAndBlocks[blockNumber]
                    listOfDataNodes.append(str(activeDataNodes[dataNodeNum]))
                    nodesAndBlocks[blockNumber] = listOfDataNodes
                    dataNodeNum += 1
            blockNumber += 1
        files[filePath] = nodesAndBlocks  # Add block list of file to files dict()
        print("block list:", nodesAndBlocks)
        return nodesAndBlocks, 200

    def get(self, filePath):
        if filePath in files:
            return files[filePath]
        else:
            return None, HTTPStatus.NotFound.code


# Retrieve a list of blocks and DataNodes
# Request: GET /getFileBlocks/amazon_reviews_us_Electronics_v1_00.tsv.gz
# Response: {"amazon_reviews_us_Electronics_v1_00.tsv.gz.block-0": ["Node1"], "amazon_reviews_us_Electronics_v1_00.tsv.gz.block-1": ["Node1"]}
class GetFileBlocks(Resource):
    def get(self, file_name_query):
        blocklist = {}  # block list from files
        # retreive a list of active DNs
        active_DN = getActiveDataNodes()

        # for testing purpose
        # active_DN = ["Node1", "Node2"]
        # for file_name, block_info in files.items():
        #     # print (block_info)
        #     if file_name == file_name_query:
        #         for block_id, node_lists in block_info.items():
        #             print (node_lists)
        #             for node_id in node_lists:
        #                 if node_id in active_DN:
        #                     if block_id in blocklist:
        #                         blocklist[block_id].append(node_id)
        #                     else:
        #                         blocklist[block_id] = [node_id]
        blocklist = files[file_name_query]
        print(blocklist)
        if not bool(blocklist):
            return HTTPStatus.NotFound
        else:
            return blocklist


# Method to delete a specified file, delete file from all DataNodes
# Delete all target file blocks in target DataNodes
class DeleteFile(Resource):
    def delete(self):
        filePath = request.form['file_path']
        if filePath in files:
            # delete file blocks in all DataNodes
            info = files[filePath]
            blocks = info.keys()
            for block in blocks:  # loop through all blocks for the target file
                for dn in block.values():  # loop through all DataNodes for each block
                    r = requests.delete('http://' + dn + ':5000/deleteBlock/', json={
                        "Target": block})  # send delete request to DataNode with target file block
                    if r.status_code == 200:
                        print(block + " deletion completed at DataNode " + dn)
                    else:
                        print(block + " deletion failed at DataNode " + dn)
            del files[filePath]
            return (filePath + ' deletion completed!')
        else:
            # error - file does not exists / no such directory
            # os.remove(filePath)
            return None, HTTPStatus.NotFound.code


# Method to retrieve the list of blocks and DataNodes that store each block from the NameNode and display them with a given filename
# Assumes that:
# 1 - the client provides the fileName
# 2 - directories and files have different names
# 3 - no duplicate files exists in different directories
# sample test on postman: GET http://localhost:5000/list/amazon_reviews_us_Electronics_v1_00.tsv.gz should output the info stored in files{}
class List(Resource):
    def get(self):
        fileName = request.form['file_name']
        for name in files.keys():
            if fileName in name:
                return files[name]
        return ("Cannot find matched file " + fileName), 404


# Method to list all subdirectories and files under a specified path
# For root direcotry look up use ->  ./
# For regular look up use -> ./targetDir
class ListAll(Resource):
    def get(self):
        filePath = request.form['file_path']
        if filePath == "./":
            return os.listdir('.')
        elif os.path.exists(filePath):
            dirs = os.listdir(filePath)
            return dirs
        else:
            return ("FilePath does not exists."), 404


# Method to receive block report from a DataNode
class ReceiveBlockReport(Resource):
    def post(self, DataNodeID):
        response = request.get_json()
        recentDataNodes[DataNodeID] = time.time()
        files[DataNodeID] = {
            "BlockList": response["Blockreport"]
        }


# Method to make new directory
class MakeDirectory(Resource):
    def post(self):
        dir = request.form['file_path']  # the target directory trying to create
        if os.path.isdir(dir) or os.path.exists(dir):
            return None, 409  # conflict if directory already exists
        else:
            os.mkdir(dir)
            return 200


class getDataNodeInfo(Resource):
    def post(self):
        global blockReportInfo
        data = request.json
        DataNodeID = data['DataNodeID']
        recentDataNodes[DataNodeID] = time.time()
        list_of_blocks = data['list_of_blocks']
        blockReportInfo[DataNodeID] = list_of_blocks
        print("got the ip : ", DataNodeID)
        print("got the list: ", list_of_blocks)
        return None, 200


api.add_resource(getDataNodeInfo, '/getDataNodeInfo')


# Method to remove directory if it's empty
class RemoveDirectory(Resource):
    def delete(self):
        dir = request.form['file_path']  # the target directory trying to create
        if os.path.exists(dir):
            if ListAll.get(dir) == []:
                os.rmdir(dir)
                return ("Target directory removed."), 200
            else:
                return ("Target directory is not empty."), 409
        else:
            return ("FilePath does not exists."), 404


api.add_resource(RemoveDirectory, '/removeDirectory')
api.add_resource(MakeDirectory, '/directories')
api.add_resource(CreateFile, '/createFile/')
api.add_resource(GetFileBlocks, '/getFileBlocks/<string:file_name_query>')
api.add_resource(ReceiveBlockReport, '/revBlockReport/<string:DateNodeID>')
api.add_resource(DeleteFile, '/deleteFile')
api.add_resource(ListAll, '/listAll')
api.add_resource(List, '/list')

if __name__ == '__main__':
    # app.run(debug=True)
    t = threading.Thread(target=handleNodeFailure)
    t.start()
    app.run(host=os.getenv('LISTEN', '0.0.0.0'))