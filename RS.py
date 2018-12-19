import socket
import threading
import sys
import os
import time
import random
from message import *
from peer_record import *

#***********************************************************************************************************************************************************************

def gen_cookie():                                                                                      
    ''' Function to create a random cookie but ensure that its not already present in peer_index'''
    global peer_index
    uniq_cookie = 1
    while True:
        r_cookie = random.randrange(start = 10000, stop = 99999, step = 1)
        for peer in peer_index:
            if r_cookie == int(peer.cookie):
                uniq_cookie = 0
        if uniq_cookie:
            break
    return r_cookie

def day_limit(peer_time_str):
    '''Function to take time strings and convert them into time tuples and compare if the day of this time is less than or equal to 30 days before today'''
    peer_time = time.strptime(peer_time_str, '%a %b %d %H:%M:%S %Y')                                        # Converting string times into time tuples
    now_time = time.strptime(time.asctime(), '%a %b %d %H:%M:%S %Y')

    if now_time[7] - peer_time[7] <= 30:                                                                    # Same hour value but different minutes value
        return True
    else:
        return False

#***********************************************************************************************************************************************************************

#                                                                        H A N D L E R    F U N C T I O N

def handler(conn_socket, ip_addr):                                                                     
    '''Reads the message and acts on it and sends the specific response'''
    global peer_index, peer_list_file
    raw_data = conn_socket.recv(1024).decode()
    print "New message received\n",raw_data
    in_msg = message()
    in_msg.create_fields(raw_data)
    
    new_peer_flag = 0   
    if in_msg.mtype == 'Register':                                                                     # Register the peer in the peer index
        new_peer_flag = 1
        for peer in peer_index:                                                                        # Exception case checking : If peer already in peer index but sends Register message 
            if in_msg.hostname == peer.hostname:
                old_peer_index = peer_index.index(peer)
                new_peer_flag = 0
            
        if new_peer_flag:
            peer_welcome_port = in_msg.headervalue
            last_contacted = time.asctime()
            active_count = 1                                                                            # do this only if its within the last 30 mins else this is the first time contacted
            flag = True
            ttl = 7200
            cookie = gen_cookie()
            new_peer = peer_record(in_msg.hostname, cookie, flag, ttl, peer_welcome_port, active_count, last_contacted)              # Create new peer record
            peer_index.append(new_peer)                                                                # Append the new peer record into the peer_index list
            peer_list = open(peer_list_file, mode = 'a')
            peer_str = new_peer.peer_string() + '--'
            peer_list.write(peer_str)
            peer_list.close()
        else:                                                                                          # Overwrite on the existing peer record
            peer = peer_index[old_peer_index]
            peer_index.remove(peer)
            peer.flag = True
            cookie = peer.cookie
            peer.peer_welcome_port = in_msg.headervalue
            if day_limit(peer.last_contacted):
               peer.active_count = int(peer.active_count)+ 1
            else:
               peer.active_count = 1
            peer.last_contacted = time.asctime()
            peer_index.insert(old_peer_index, peer)
            peer_list = open(peer_list_file, mode = 'w')
            for peer in peer_index:
                peer_str = peer.peer_string() + '--'
                peer_list.write(peer_str)
            peer_list.close()
            
        out_msg_reg = message()                                                                        
        out_msg_reg.mtype = 'RegisterReply'
        out_msg_reg.statuscode = 'OK'
        out_msg_reg.hostname = server
        out_msg_reg.headertag='Cookie'
        out_msg_reg.headervalue=str(cookie)
        out_msg_reg.data = ''
        out_msg_reg.create_raw()
        print "RegisterReply sent:", out_msg_reg.raw,"@@@@\n"        
        conn_socket.send(out_msg_reg.raw.encode())                                                       # Send reply message to peer-client
    
    elif in_msg.mtype == 'PQuery':
        out_msg_pqy = message()
        out_msg_pqy.mtype = 'PQueryReply'
        out_msg_pqy.statuscode = 'OK'
        out_msg_pqy.hostname = server
        out_msg_pqy.headertag=''
        out_msg_pqy.headervalue=''
        peer_sep = '--'
        peer_reply = ''

        peer_list = open(peer_list_file, mode = 'r+')                                                           # opens the file peer_list_file for reading or writing and created for the first time
        peer_string_index = [line.split('--') for line in peer_list.readlines()]                                # reads the peer_list_file and stores into the list peer_string_index the peer strings
        if peer_string_index:
            peer_string_index = peer_string_index[0]
            peer_string_index = peer_string_index[0:len(peer_string_index)-1]

        peer_list.close()

        peer_index = []

        while peer_string_index:
            peer_str = peer_string_index.pop(0)
            peer_attr = peer_str.split('*')
            peer = peer_record(peer_attr[0], peer_attr[1], peer_attr[2], peer_attr[3], peer_attr[4], peer_attr[5], peer_attr[6])
            peer_index.append(peer)
        for peer in peer_index:                                                                    # Extract each peer in peer_index
            if peer.flag == 'True':
                if peer.hostname != in_msg.hostname:
                    peer_reply += peer.peer_string() + str(len(peer_index)) + peer_sep                                     # Create single string with peer data separated by '--' to insert into data field of reply message
        out_msg_pqy.data = peer_reply
        out_msg_pqy.create_raw()
        print "PQueryReply sent:", out_msg_pqy.raw,"@@@@\n"
        conn_socket.send(out_msg_pqy.raw.encode())
			
    elif in_msg.mtype == 'KeepAlive':                                                                   # Update the TTL field of the specific peer in the peer index if headertag, 'close' is 0 else pop the entry
        peer_list = open(peer_list_file, mode = 'w')
        for peer in peer_index:
            if in_msg.hostname == peer.hostname:                                                       # Exception Case Checking : If peer-client that is not there in peer_index sends KeepAlive message send back ERR
                i = peer_index.index(peer)
                peer_index.remove(peer)
                peer.ttl = 7200
                peer.flag = True
                peer.active_count += 1
                peer_index.insert(i, peer)
            peer_str = peer.peer_string() + '--'
            peer_list.write(peer_str)
        peer_list.close()

        out_msg = message()
        out_msg.type = 'KeepAliveReply'
        out_msg.statuscode = 'OK'
        out_msg.hostname = server
        out_msg.headertag = ''
        out_msg.headervalue = ''
        out_msg.data = ''
        out_msg.create_raw()		    
        print "KeepAliveReply sent:", out_msg.raw,"@@@@\n"
        conn_socket.send(out_msg.raw.encode())

    elif in_msg.mtype == 'Leave':
        peer_list = open(peer_list_file, mode = 'w')
        for peer in peer_index:
            if in_msg.hostname == peer.hostname:                                                  
                i = peer_index.index(peer)
                peer_index.remove(peer)
                peer.flag = False
                peer_index.insert(i, peer)
            peer_str = peer.peer_string() + '--'
            peer_list.write(peer_str)
        peer_list.close()
        out_msg = message()
        out_msg.type = 'LeaveReply'
        out_msg.statuscode = 'OK'
        out_msg.hostname = server
        out_msg.headertag = ''
        out_msg.headervalue = ''
        out_msg.data = ''
        out_msg.create_raw()		    
        print "LeaveReply sent:\n", out_msg.raw,"@@@@\n"
        conn_socket.send(out_msg.raw.encode())
        
    conn_socket.close()
    return None                                                                                       # Close connection after handling

def update_ttl():
    global peer_list_file, peer_index
    past_time_secs = 0
    file_write = False
    while True:
        present_time_secs = int(time.clock())
        if present_time_secs - past_time_secs == 1:                                                         # for every second, update TTL value of each peer record.
            for peer in peer_index:
                i = peer_index.index(peer)
                peer_index.remove(peer)
                peer.ttl = str(int(peer.ttl) - 1)
                if int(peer.ttl) <= 0:                                                                            # Insert peer record back into peer_index only if TTL greater than 0, else don't
                    peer.flag = False
                    file_write = True
                peer_index.insert(i, peer)
            if file_write:  
                peer_list = open(peer_list_file, mode = 'w')
                for a_peer in peer_index:
                    a_peer_str = a_peer.peer_string() + '--'
                    peer_list.write(a_peer_str)
                peer_list.close()
                file_write = False
            past_time_secs = present_time_secs


#**********************************************************************************************************************************************************************

#                                                                 L O A D I N G    O F    P E E R    I N D E X 
global peer_index, peer_list_file
peer_list_file = 'peer_list_file.txt'
try:
    peer_list = open(peer_list_file, mode = 'r+')                                                           # opens the file peer_list_file for reading or writing and created for the first time
    peer_string_index = [line.split('--') for line in peer_list.readlines()]                                # reads the peer_list_file and stores into the list peer_string_index the peer strings
    if peer_string_index:
        peer_string_index = peer_string_index[0]
        peer_string_index = peer_string_index[0:len(peer_string_index)-1]

    peer_list.close()

    peer_index = []

    while peer_string_index:
        peer_str = peer_string_index.pop(0)
        peer_attr = peer_str.split('*')
        peer = peer_record(peer_attr[0], peer_attr[1], peer_attr[2], peer_attr[3], peer_attr[4], peer_attr[5], peer_attr[6])
        peer_index.append(peer)
except:
    peer_list = open(peer_list_file, mode ='w')
    peer_list.close()
    peer_index = []

#************************************************************************************************************************************************************************

#                                                                  E X E C U T I O N    O F    M A I N    C O D E

global server, port
server = '10.25.5.87'
port = 65423

ttl_thread = threading.Thread(target = update_ttl, args = '')
ttl_thread.start()

server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server_socket.bind((server, port))                                                                     # binding this socket to the welcome port
server_socket.listen(1)                                                                                # listens on this socket for incoming connection requests
print "Server listening on ", port,"..."
while True:
    connection_socket, peer_addr = server_socket.accept()                                               # accept any connection requests that arrive at welcome port
    print "New connection received from ", peer_addr[0]
    connection_thread = threading.Thread(target = handler, args = (connection_socket, peer_addr[0]))    # create a new thread to handle this connection. The connection is handled by handler()
    connection_thread.daemon = True                                                                    
    connection_thread.start()
