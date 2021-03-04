# Cloud9-SUFS
Seattle University File System based on HDFS

This project was originally collaborated and developed on AWS CodeCommit

test namenode createfile from terminal, ex command:

curl http://localhost:5000/createFile/test2 -d "fileSize=1048577" -X PUT
curl http://localhost:5000/createFile/test2 -X GET
curl http://localhost:5000/deleteFile/test2 -X DELETE

# Start namenode
use this command `python NameNode.py`

# Start the DataNode
use this following command. This program takes 2 arguments, 1st is DataNode Port Number and 2nd is NameNode IP
`python DataNode.py 5000 localhost`

# Test sending a blockdata to DataNode:
In postman, make a POST request to `http://localhost:5000/BlockData/<BlockID>` The BlockID will be the filename.

Sample Request:
POST http://localhost:5000/BlockData/test
{"data": "Hello World.", "size": 12}

# Test receiving a blockdata to DataNode:
In postman, make a GET request to `http://localhost:5000/BlockData/<BlockID>` The BlockID will be the filename.

Sample Request:
GET http://localhost:5000/BlockData/test

# Test SendCopy between DataNodes:
Open three tabs of terminal:
python NameNode.py
python DataNode.py 5001 localhost
python DataNode.py 5002 localhost

In postman, make a POST request to http://0.0.0.0:5001/BlockData/test 
BlockID is "test", stored in data_blocks folder (you created) on Node 5001.
Body JSON: {"data": "Hello World.", "size": 12}
Make another POST request to SendCopy of block "test" to Node 5002 http://0.0.0.0:5001/SendCopy
Body JSON: {"target_node": "0.0.0.0:5002", "block_id": "test"}
Success.
You may check GET on http://0.0.0.0:5002/BlockData/test to comfirm it's sent there.


# Test DataNode delete file block
In postman, first create a test file with:
    POST http://localhost:5000/BlockData/test
    {"data": "Hello World.", "size": 12}
Then create a delete request 
    DELETE http://localhost:5000/deleteBlock
    {"Target": "test"}
The test file should get deleted

# Test NameNode list
In postman, test with:
    GET http://localhost:5000/list/amazon_reviews_us_Electronics_v1_00.tsv.gz 
The output should match the sample data on the files{}

# Test on the list in Client
In terminals, run both NameNode.py and Client.py
In client terminal, type "list",
    For valid test, type file name as ->  amazon_reviews_us_Electronics_v1_00.tsv.gz
    For invalid test, type file name as ->  asd
    Valid result should match files{}, invalid result should output error message.

# Test on ls in Client
In terminal, run both NameNode.py and Client.py
In Client terminal, type "ls",
    For root folder look up, use input:  ./
    For ls in sub-directory, use input: ./targetDir   or   targetDir

# Test on rmdir in Client
In terminal, run both NameNode.py and Client.py
Create couple subfolders, ex. create a test folder, then create a test2 under test folder.
    current folder dir: ./test/test2
In Client terminal, type "rmdir",
    We can remove test2 folder, use input:  ./test/test2
    Note that folder will be removed if it's empty, otherwise error message will pop up.

