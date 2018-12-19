import socket
import threading
import time
import sys
import os
from message import *
from peer_record import *
from rfc_record import *    

#**************************************************************************************************************************************************************

#                                                    R F C    H A N D L E R

def rfc_handler(conn_socket, ip_addr):
    '''Function which receives the connection and processes the incoming message and replies to RFC_client'''
    global rfc_list, rfc_list_file, rfc_index
    
    raw_data = conn_socket.recv(1024).decode() 
    in_msg = message()
    in_msg.create_fields(raw_data)
    print "Message received:", raw_data,"|||\n"
    if in_msg.mtype == 'RFCQuery':                                                                          # if the incoming message is of mtype RFCQuery, we must check if the requested RFC ID is present in RFC index and return this list
        req_rfc_id = in_msg.data
        out_msg = message()
        out_msg.mtype = 'RFCQueryReply'
        out_msg.statuscode = 'OK'
        out_msg.hostname = rfc_server
        out_msg.headertag = 'NoOfOccurrences'                                                           # If the peer has the  entry in RFCIndex, then this header tag has a non-zero header value
        rfc_rec_sep = '--'
        occurrences = 0
        reply_list = ''
        for rfc_rec in rfc_index:                                                                          # Extract each rfc_rec in rfc_index
            if req_rfc_id == str(rfc_rec.id):                                                                   # check if the rfc ID matches
                reply_list += rfc_rec.rfc_rec_string() + rfc_rec_sep                                         # Create single string with rfc_rec data separated by '--' to insert into data field of reply message
                occurrences += 1
        print "Occurrences:",occurrences
        out_msg.headervalue = str(occurrences)                                                          # If multiple occurrences are there then this is non-zero
        out_msg.data = reply_list
        out_msg.create_raw()
        print "RFCQueryReply sent:", out_msg.raw, "|||\n"
        conn_socket.send(out_msg.raw.encode())

    elif in_msg.mtype == 'GetRFC':                                                                          # If incoming message is GetRFC then first check if the RFC is indeed present and if yes then send 'OK' GetRFCReply message and then send RFC
        rfc_file_name = 'rfc' + in_msg.data + '.txt'
        for rfc_rec in rfc_index:
            if in_msg.data == rfc_rec.id:
               break
            else:
                pass
        file_present = False
        try:                                                                                               # Try opening the RFC file from memory if not then it is absent and thus send 'ERR' GetRFCReply message
            rfc_file = open(rfc_file_name, mode = 'rb')                                                    # If RFC file absent on memory (deleted after RFCQuery), will raise error
            file_present = True
        except:
            file_present = False
        if file_present:
            out_msg = message()                                                                            
            out_msg.mtype = 'GetRFCReply'
            out_msg.statuscode = 'OK'
            out_msg.hostname = rfc_server
            out_msg.headertag = ''
            out_msg.headervalue = ''
            out_msg.data = rfc_file.read(204800)
            out_msg.create_raw()
            print "GetRFCReply sent:", out_msg.raw, "|||\n"
            rfc_file.close()
            conn_socket.send(out_msg.raw.encode('utf-8'))
            
            rfc_rec_new = rfc_rec                                                                          # Make an entry into RFC index that this new peer also has this RFC
            rfc_rec_new.hostname = ip_addr
            rfc_index.append(rfc_rec_new)                                                                 # Make an entry in RFC index that this RFC is now available locally
            print "Length of rfc index:",len(rfc_index)
            rfc_list  = open(rfc_list_file, mode = 'w')
            print "Recreating the rfc_list_file:"
            for rfc_rec in rfc_index:
                rfc_rec_str = rfc_rec.rfc_rec_string() + '--'
                print "rfc_rec_str:", rfc_rec_str 
                rfc_list.write(rfc_rec_str)
            rfc_list.close()
        else:                                                                                            # Exception case if the RFC is not present on this peer. Note here that we can include a search into this peer's RFC index and return any successful results**
            out_msg = message()                                                                              
            out_msg.mtype = 'GetRFCReply'
            out_msg.statuscode = 'ERR'
            out_msg.hostname = rfc_server
            out_msg.headertag = ''
            out_msg.headervalue = ''
            out_msg.data = ''
            out_msg.create_raw()
            print "GetRFCReply sent----------------------->|||\n"
            conn_socket.send(out_msg.raw.encode())

    else:
        out_msg = message()                                                                                # Exception Case Checking : If an invalid message is sent to RFC server
        out_msg.mtype = 'InvalidMsgmtypeReply'
        out_msg.statuscode = 'ERR'
        out_msg.hostname = rfc_server
        out_msg.headertag = ''
        out_msg.headervalue = ''
        out_msg.data = ''
        out_msg.create_raw()

        conn_socket.send(out_msg.raw.encode())

    conn_socket.close()                                                                                    # Close connection after handling

def update_ttl():
    global rfc_list_file, rfc_index
    past_time_secs = 0
    file_write = False
    while True:
        present_time_secs = int(time.clock())
        if present_time_secs - past_time_secs == 1:                                                         # for every second, update TTL value of each peer record.
            for rfc_rec in rfc_index:
                i = rfc_index.index(rfc_rec)
                rfc_index.remove(rfc_rec)
                rfc_rec.ttl = str(int(rfc_rec.ttl) - 1)
                if int(rfc_rec.ttl) <= 0:                                                                            # Insert peer record back into peer_index only if TTL greater than 0, else don't
                    rfc_rec.flag = False
                    file_write = True
                rfc_index.insert(i, peer)
            if file_write:  
                rfc_list = open(rfc_list_file, mode = 'w')
                for a_rfc_rec in rfc_index:
                    a_rfc_rec_str = a_rfc_rec.rfc_rec_string() + '--'
                    rfc_list.write(a_rfc_rec_str)
                rfc_list.close()
                file_write = False
            past_time_secs = present_time_secs

def create_title(filename):
    rfc_file = open(filename, mode ='r')
    content = rfc_file.read()
    rfc_file.close()
    end = content.index('Abstract')
    title_end = end-2
    marker = end
    while marker >= 0:
        if content[marker-3:marker] == '\n\n\n':
            break 
        marker-=1
    title_start = marker
    return content[title_start:title_end]
    
#***************************************************************************************************************************************************************

rfc_list_file = 'rfc_list_file.txt'
rfc_list = open(rfc_list_file, mode = 'w')
for rfc_id in range(8199, 8277):
    rfc_filename = 'rfc'+str(rfc_id)+'.txt'
    if os.path.isfile(rfc_filename):
        with open(rfc_filename, 'r') as content_file:
            content = content_file.read()
        rfc_list = open(rfc_list_file, mode = 'a+')
        rfc_index = rfc_record(rfc_id, create_title(rfc_filename), socket.gethostbyname(socket.gethostname()), '7200')
        rfc_list.write(rfc_index.rfc_rec_string()+'--')
        rfc_list.close()

rfc_list = open(rfc_list_file, mode = 'r')                                                           # opens the file rfc_list_file for reading or writing and created for the first time
rfc_list_content = rfc_list.read()
rfc_string_index = rfc_list_content.split('--')                                                                     # reads the rfc_list_file and stores into the list rfc_string_index the rfc strings
if rfc_string_index:
    rfc_string_index = rfc_string_index[0:len(rfc_string_index)-1]
rfc_list.close()
rfc_index = []
if rfc_string_index:
    while rfc_string_index:
        rfc_str = rfc_string_index.pop(0)
        rfc_attr = rfc_str.split('*')
        rfc_rec = rfc_record(rfc_attr[0], rfc_attr[1], rfc_attr[2], rfc_attr[3])
        rfc_index.append(rfc_rec)

print "\nRFC index loaded from file. Length of RFC_index is:", len(rfc_index)

#***********************************************************************************************************************************************************************

#                                                     E X E C U T I O N    O F    M A I N    C O D E


rfc_server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
RFC_Server_port = 1257                                                                             # common welcome port on all RFC servers
rfc_server = socket.gethostbyname(socket.gethostname())
rfc_server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
rfc_server_socket.bind((rfc_server, RFC_Server_port))                                                        # binding this socket to the welcome port

rfc_server_socket.listen(1)                                                                                # listens on this socket for incoming connection requests
print "RFC server listening on port ",RFC_Server_port,"\n"

#****************************************************************************************************************************************************************    

past_time_secs = 0

while True:
    rfc_connection_socket, peer_addr = rfc_server_socket.accept()                                              # accept any connection requests that arrive at welcome port
    print "New connection received from:", peer_addr[0], "\n"
    rfc_connection_thread = threading.Thread(target = rfc_handler, args = (rfc_connection_socket, peer_addr[0]))  # create a new thread to handle this connection. The connection is handled by rfc_handler()
    rfc_connection_thread.daemon = True                                                                    
    rfc_connection_thread.start()
