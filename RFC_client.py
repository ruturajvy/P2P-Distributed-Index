import socket
import threading
import sys
import time
from message import *
from peer_record import *
from rfc_record import *
    
#*********************************************************************************************************************************************************************************************************************

def P2P_registration():
    '''Function which sends the initial Register message'''
    global RS_server, RS_port, RFC_client, RFC_Server_port, cookie
    
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
    client_socket.connect((RS_server, RS_port))
    RFC_client = client_socket.getsockname()[0]
    in_msg_reg = message()
    in_msg_reg.statuscode = 'ERR'
    out_msg_reg = message()
    out_msg_reg.mtype = 'Register'
    out_msg_reg.statuscode = 'OK'
    out_msg_reg.hostname = RFC_client
    out_msg_reg.headertag ='WelcomePort'                                                                                 # should assign this dynamically based on rfc server port value
    out_msg_reg.headervalue = str(RFC_Server_port)
    out_msg_reg.data = ''
    out_msg_reg.create_raw()
    print "\nRegistration message sent:", out_msg_reg.raw,"|||\n"
    client_socket.send(out_msg_reg.raw.encode())                                                            # Send the Register message
    RS_reply = client_socket.recv(1024).decode()                                                         # Get the reply from the RS for the register message
    in_msg_reg.create_fields(RS_reply)                                                                   
    cookie = in_msg_reg.headervalue
    print "\nRegistration reply received:", in_msg_reg.raw,"|||\n"
    client_socket.close()
    time.sleep(20)
    return None

#**************************************************************************************************************************************************************

#                                                                        U S E R    D I S P L A Y
    
def user_display():
    global RS_server, RS_port, RFC_client, cookie, shut
    global peer_index, rfc_index
    global peer_list_file, rfc_list_file, peer_list, rfc_list
    log_file_name = 'log_file.txt'
    log_file = open(log_file_name, mode = 'w')
    log_file.write('*TESTING*\n')

    req_rfc_list = range(8199,8277)
    user_input = 'a'
    print "P2P RFC Transfer Client\n"
    print "-----------------------\n"
    print "Welcome to P2P RFC Transfer Client\n"
    while req_rfc_list:
        print "User Menu"
        print "1.Request next RFC (R or r)\n"
        print "2.Leave (Q or q)\n"
        #user_input = raw_input("Enter your choice:")
        user_input = 'r'
        if user_input == 'R' or user_input == 'r':
            log_file = open(log_file_name, mode = 'a+')
            log_file.write('\n----Requested:'+str(time.clock()))
            rfc_list_file = 'rfc_list_file.txt'
            create_rfc_list = False
            try:
                rfc_list = open(rfc_list_file, mode = 'r')                                                           # opens the file rfc_list_file for reading or writing and created for the first time
                create_rfc_list = True
            except:
                create_rfc_list = False
            if create_rfc_list:
                rfc_string_index = [line.split('--') for line in rfc_list.readlines()]                                # reads the rfc_list_file and stores into the list rfc_string_index the rfc strings
                if rfc_string_index:
                    rfc_string_index = rfc_string_index[0]
                    rfc_string_index = rfc_string_index[0:len(rfc_string_index)-1]
                
                rfc_list.close()
                rfc_index = []
                while rfc_string_index:
                    rfc_str = rfc_string_index.pop(0)
                    rfc_attr = rfc_str.split('*')
                    rfc_rec = rfc_record(rfc_attr[0], rfc_attr[1], rfc_attr[2], rfc_attr[3])
                    rfc_index.append(rfc_rec)
            else:
                rfc_index = []
                rfc_list = open(rfc_list_file, mode ='w')
                rfc_list.close()
              
            req_rfc_id = str(req_rfc_list.pop(0))
            rfc_file_name = 'rfc' + req_rfc_id + '.txt'
            in_msg_gtrfc = message()
            rfc_found = 0
            for rfc_rec in rfc_index:                                                                                    # Check if the required rfc is present in rfc_index
                if req_rfc_id == str(rfc_rec.id) and rfc_rec.hostname == RFC_client:
                    rfc_found = 1
                    print "RFC found locally\n"
                    break
					
            if not(rfc_found):                                                                                           # Check if the required rfc is present at a remote port
                out_msg_pqy = message()
                out_msg_pqy.mtype = 'PQuery'
                out_msg_pqy.statuscode = 'OK'
                out_msg_pqy.hostname = RFC_client
                out_msg_pqy.headertag = 'Cookie'
                out_msg_pqy.headervalue = str(cookie)
                out_msg_pqy.data = ''
                out_msg_pqy.create_raw()
                print "PQuery message sent\n", out_msg_pqy.raw
                pquery_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                pquery_socket.connect((RS_server, RS_port))
                pquery_socket.send(out_msg_pqy.raw.encode())                                                            # Send the PQuery message
                RS_reply = pquery_socket.recv(1024).decode()                                                             # Obtain the PQuery reply
                pquery_socket.close()
                print "PQuery Reply received\n", RS_reply
                in_msg_pqy = message()
                in_msg_pqy.create_fields(RS_reply)

                peer_string_index = in_msg_pqy.data.split('--')                                                          # Create the peer_index from the PQuery reply
                
                if peer_string_index:
                    peer_string_index = peer_string_index[0:len(peer_string_index)-1]

                peer_index = []

                for peer_str in peer_string_index:
                    peer_attr = peer_str.split('*')
                    peer = peer_record(peer_attr[0], peer_attr[1], peer_attr[2], peer_attr[3], peer_attr[4], peer_attr[5], peer_attr[6])
                    peer_index.append(peer)

                peer_list_file = 'peer_list_file.txt'                                                                   # Write the peer index into the peer_list_file
                peer_list = open(peer_list_file, mode = 'w')
                for peer in peer_index:
                    peer_str = peer.peer_string()
                    peer_list.write(peer_str)
                peer_list.close()
                
                out_msg_rqy = message()
                out_msg_rqy.mtype = 'RFCQuery'
                out_msg_rqy.statuscode = 'OK'
                out_msg_rqy.hostname = RFC_client
                out_msg_rqy.headertag = ''
                out_msg_rqy.headervalue = ''
                out_msg_rqy.data = req_rfc_id
                out_msg_rqy.create_raw()

                for peer in peer_index:                                                                                  # send RFCQuery to every peer in peer_index sequentially
                    print "Trying to contact peer(RFCQuery)--->", peer.hostname
                    conn_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    conn_socket.connect((peer.hostname, int(peer.peer_welcome_port)))
                    conn_socket.send(out_msg_rqy.raw.encode())
                    print "RFCQuery message sent to ",peer.hostname, "-->", out_msg_rqy.raw, "|||\n"
                    in_msg_rqy = message()
                    peer_reply = conn_socket.recv(1024).decode()                                                         # RFCQuery reply from RFC server
                    print "RFCQuery reply received:",peer_reply,"|||\n"
                    conn_socket.close()
                    
                    in_msg_rqy.create_fields(peer_reply)

                    if int(in_msg_rqy.headervalue) > 0:                                                                  # if the peer replies with a message that has NoOfOccurrences headertag greater than 0 then send GetRFC to the list of peers that appear in the rfc_list
                        req_rfc_string_index = in_msg_rqy.data.split('--')                                               # split lines in rfc_list_file into individual strings that are RFC index records
                        req_rfc_index = []
                        
                        if req_rfc_string_index:
                            req_rfc_string_index = req_rfc_string_index[0:len(req_rfc_string_index)-1]
                        while req_rfc_string_index:                                                                      # loop to read into each line and create list of objects of mtype rfc_record
                            req_rfc_attr = req_rfc_string_index.pop(0).split('*')
                            req_rfc_rec = rfc_record(req_rfc_attr[0], req_rfc_attr[1], req_rfc_attr[2], req_rfc_attr[3])
                            req_rfc_index.append(req_rfc_rec)
                            rfc_index.append(req_rfc_rec)                                                                 # append the entry to your own RFC index

                        for req_rfc_rec in req_rfc_index:                                                                    # send GetRFC to each peer present in rfc_list
                            print "req_rfc_rec:",req_rfc_rec
                            for a_peer in peer_index:                                                                      # find peer_welcome_port for that peer
                                print "a_peer.hostname:",a_peer.hostname
                                if req_rfc_rec.hostname == a_peer.hostname:
                                    print "Found a match:",req_rfc_rec.hostname,"at",int(a_peer.peer_welcome_port)
                                    getrfc_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                    getrfc_socket.connect((req_rfc_rec.hostname,int(a_peer.peer_welcome_port)))
                                    break
                            out_msg_gtrfc = message()
                            out_msg_gtrfc.mtype = 'GetRFC' 
                            out_msg_gtrfc.statuscode = 'OK'
                            out_msg_gtrfc.hostname = RFC_client
                            out_msg_gtrfc.headertag = ''
                            out_msg_gtrfc.headervalue = ''
                            out_msg_gtrfc.data = req_rfc_id
                            out_msg_gtrfc.create_raw()
                            print "GetRFC message sent:", out_msg_gtrfc.raw, "|||\n"
                            getrfc_socket.send(out_msg_gtrfc.raw.encode())                                                # send the GetRFC message

                            peer_reply = getrfc_socket.recv(204800).decode('utf-8')                                                      # when reply is encountered store it in peer_reply
                            in_msg_gtrfc = message()
                            in_msg_gtrfc.create_fields(peer_reply)
                            print "GetRFC reply received from:", in_msg_gtrfc.hostname, "with statuscode:",in_msg_gtrfc.statuscode,"|||\n"
                            if in_msg_gtrfc.statuscode == 'OK':                                                          # if the statuscode is 'OK' then it means data has RFC file
                                print "Creating the rfc file:",rfc_file_name
                                rfc_file = open(rfc_file_name, mode='w')                                                 # create a RFC file and write the first few bytes into it
                                rfc_file.write(in_msg_gtrfc.data)
                                rfc_file.close()
                                print "RFC found remotely\n"
                                rfc_found = 1                                                                             # if rfc_found it means all the above process has been successful, so break out of rfc_list for loop    
                                break
                            getrfc_socket.close()
                        if rfc_found:                                                                                         # if rfc_found, no need to query rest of peers, break out of peer_index for loop
                            break
                
            if rfc_found:
                log_file.write('----Retrieved:'+str(time.clock()))
                log_file.close()
                try:
                    rfc_file = open(rfc_file_name, mode = 'rb')
                    print "\nRFC file written into local database"
                    rfc_file.close()
                except:
                    print "Requested RFC not in memory"
            else:
                print "Requested RFC not in database"
            rfc_list = open(rfc_list_file, mode = 'w')
            for rfc_rec in rfc_index:
                rfc_rec_str = rfc_rec.rfc_rec_string() + '--'
                rfc_list.write(rfc_rec_str)
            rfc_list.close()
            
        elif user_input == 'Q' or user_input == 'q':
            rfc_list = open(rfc_list_file, mode = 'w')
            for rfc_rec in rfc_index:                                                                          # Convert all rfc_recs into rfc_rec_strings and write into rfc_list_file
                rfc_rec_str = rfc_rec.rfc_rec_string() + '--'
                rfc_list.write(rfc_rec_str)                                                                    # Check if it overwrites or appends. Must overwrite and not append. 
            rfc_list.close()                                                                                   # Close the rfc_list_file 
            
            out_msg_leave = message()
            out_msg_leave.mtype = 'Leave'
            out_msg_leave.statuscode = 'OK'
            out_msg_leave.hostname = RFC_client
            out_msg_leave.headertag = 'Cookie'
            out_msg_leave.headervalue = str(cookie)
            out_msg_leave.data = ''
            out_msg_leave.create_raw()
            print "Leave message sent:", out_msg_leave.raw, "|||\n"	
            in_msg_leave = message()
            leave_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            leave_socket.connect((RS_server, RS_port))
            leave_socket.send(out_msg_leave.raw.encode())                                                       # Convey to server that peer is leaving

            RS_reply = leave_socket.recv(1024).decode()                                                               # Even if server replies with ERR, leave, the server will realise late that peer has left
            print "Leave reply received:", RS_reply, "|||\n"
            cookie_file = open('cookie.txt', mode = 'w+')
            cookie_file.write(cookie)
            cookie_file.close()
            log_file.close()
            shut = True
            return	
        else:
            print "Invalid user input"
            
def KeepAlive():
    past_time_secs = 0

    while True:
        present_time_secs = int(time.clock())
        if present_time_secs - past_time_secs == 7199:                                                                        # For every 7199 seconds, send a KeepAlive message to registration server
            out_msg_kpa = message()
            out_msg_kpa.mtype = 'KeepAlive'
            out_msg_kpa.statuscode = 'OK'
            out_msg_kpa.hostname = RFC_client
            out_msg_kpa.headertag ='Cookie'
            out_msg_kpa.headervalue = str(cookie)
            out_msg_kpa.data = ''
            out_msg_kpa.create_raw()
            print "KeepAlive message sent:", out_msg_kpa.raw, "|||\n"
            keepalive_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            keepalive_socket.send(out_msg_kpa.raw.encode())
    
            rs_reply = keepalive_socket.recv(1024).decode() 
            print "KeepAlive reply received:", peer_reply,"|||\n"            
            past_time_secs = present_time_secs
    
#***************************************************************************************************************************************************************

#                                                    E X E C U T I O N   O F   M A I N    C O D E 

RS_server = '10.25.5.87'                                                                                                         # Enter this address***
RS_port = 65423
RFC_Server_port =1257
shut = False
cookie = '00000'
rfc_list_file = 'rfc_list_file.txt'
rfc_index = []

P2P_registration()                                                                                                       # Initial registration with the RS server with conn_socket

display_thread = threading.Thread(target = user_display, args = '')
display_thread.start()

keepalive_thread =   threading.Thread(target = KeepAlive(), args = '')
keepalive_thread.start()

while True:
    if shut == True:
        print "Thanks for using P2P client\nNow shutting down..."
        sys.exit(1)
