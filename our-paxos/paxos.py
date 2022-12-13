#!/usr/bin/env python3
import sys
import socket
import struct
from threading import Thread
from time import sleep

ACCEPTORS_AMOUNT = 3
QUORUM_AMOUNT = int(ACCEPTORS_AMOUNT/2) + 1
TIMEOUT = 0.5

def mcast_receiver(hostport):
    """create a multicast socket listening to the address"""
    recv_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    recv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    recv_sock.bind(hostport)

    mcast_group = struct.pack("4sl", socket.inet_aton(hostport[0]), socket.INADDR_ANY)
    recv_sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mcast_group)
    return recv_sock


def mcast_sender():
    """create a udp socket"""
    send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    return send_sock


def parse_cfg(cfgpath):
    cfg = {}
    with open(cfgpath, 'r') as cfgfile:
        for line in cfgfile:
            (role, host, port) = line.split()
            cfg[role] = (host, int(port))
    return cfg


def paxos_encode(loc):
    """Encode a paxos message into binary, to be sent through a network
    socket.

    loc -- list of chunks, a list of integers containing Paxos' phase 
    info, including consensus instance number
    
    Message structure: 
    every chunk <..> is 2 bytes = int range: 0-65535
    
    <instance_number><phase_ID>PHASE_PAYLOAD
    
    phase ID is:
    1: phase 1A when receiving, 1B when sending
    2: phase 2A when receiving, 2B when sending
    
    PHASE_PAYLOAD is:
    1A: <c-rnd>              total = 3 chunks
    1B: <rnd><v-rnd><v-val>  total = 5 chunks
    2A: <c-rnd><c-val>       total = 4 chunks
    2B: <v-rnd><v-val>       total = 4 chunks
    """
    msg = nbytes = 0

    for elem in loc:
        ## ! supporting integers only
        if isinstance(elem, int):
            msg = msg << 16 | elem ## 2 bytes shift
            nbytes += 2
        else:
            raise Exception('Expected list of integers as argument')

    # put size as last element
    msg = msg << 16 | len(loc)
    nbytes += 2
    #encode
    return msg.to_bytes(nbytes, 'big', signed=True)


def paxos_decode(msg_bin):
    """Decode a paxos binary message received from a network socket 
    into a loc (list of chunks), maintaining message order

    msg_bin -- encoded paxos message in binary
    """
    msg = int.from_bytes(msg_bin, byteorder='big', signed=True)
    loc = [] #list of chunks
    # original list size encoded in last chunk of the message
    num_of_chunks = msg & 2**16 -1 #bitmask last 2 bytes
    msg = msg >> 16

    # extract 2bytes chunks in the message
    for i in range(num_of_chunks):
        loc.insert(0, msg & 2**16 -1) #bitmask last 2 bytes
        msg = msg >> 16
    
    return loc

# ----------------------------------------------------

def acceptor(config, id):
    print ('-> acceptor', id)
    # dictionary of acceptor states where
    # { paxos_instance: state }
    paxos_instances = {} 
    
    r = mcast_receiver(config['acceptors'])
    s = mcast_sender()

    # timeout when 2A messages are lost
    def acceptor_timeout(id):
        sleep(TIMEOUT)
        state = paxos_instances[id]

        # have we received a phase 2A message?
        while (state['v-rnd'] != 0):
            # while not, ask proposer to restart 
            # consensus for current instance
            msg = paxos_encode([id, 5])
            s.sendto(msg, config['proposers'])

    while True:
        init_state = {"rnd": 0, "v-rnd": 0, "v-val": 0}
        # recv to a large buffer
        msg = r.recv(2**16)
        # extract the instance id and phase
        loc = paxos_decode(msg)
        id = loc[0]
        phase = loc[1]
        
        # set the initial state if it's the first time we start
        # this paxos instance
        if id not in paxos_instances.keys():
            paxos_instances[id] = init_state

        # in python changes to state (a dict) will be changed also in
        # paxos_intances (a dict of dicts). Using to avoid repeating
        # paxos_intances[instance]
        state = paxos_instances[id]
        
        if phase == 1: # received phase 1A msg from proposer
            if loc[2] > state['rnd']: # loc[2] = c-rnd
                state['rnd'] = loc[2]
                print("i: {} p: {}, state: {}".format(id, phase, state))

                # encode and send phase 1B to proposer
                msg = paxos_encode([id, phase, state['rnd'], state['v-rnd'], state['v-val']])
                s.sendto(msg, config['proposers'])

                print('*+', id)
                # start timeout on message 2A
                thread = Thread(target=acceptor_timeout, args=[id])
                thread.start()
                thread.join()

                # thread = Thread(target=learner_timeout, args=(inst_id))
                # thread.start()
                # thread.join()
                print('*+', id)
        
        elif phase == 2: # received phase 2A msg from proposer
            
             # loc[2] = c-rnd, loc[3] = c-val
             if loc[2] >= state['rnd']:
                state['v-rnd'] = loc[2]
                state['v-val'] = loc[3]
                print("i: {} p: {}, state: {}".format(id, phase, state))

                # encode and send phase 2B to proposer
                msg = paxos_encode([id, phase, state['v-rnd'], state['v-val']])
                s.sendto(msg, config['learners'])
        
        elif phase == 4: # received "resend 2B" request from learner timeout
            if state['v-rnd'] != 0:
                msg = paxos_encode([id, 2, state['v-rnd'], state['v-val']])
                s.sendto(msg, config['learners'])
        
        else:
            print('Wrong message received: unknown phase. loc: {}'.format(loc))


def proposer(config, id):
    """
    > The proposer sends a Phase 1A message to all acceptors, waits 0.5 seconds, then receives Phase 1B
    messages from acceptors until it has received 2f+1 responses. It then sends a Phase 2A message to
    all acceptors
    
    :param config
    :param id: the id of the proposer
    """
    print ('-> proposer', id)
    r = mcast_receiver(config['proposers'])
    s = mcast_sender()

    # Initialize variables for proposal
    proposal_number = 0
    proposal_value = 1

    # Send Phase 1A messages
    phase1A_msg = paxos_encode([1, 1, id])
    s.sendto(phase1A_msg, config['acceptors'])

    # Wait 0.5 seconds
    sleep(0.5)

    # Receive Phase 1B messages
    responses = []
    
    while True:
        msg = r.recv(2**16)
        loc = paxos_decode(msg)
        # print("Got:", loc)
        
        # Update proposal number and value
        if loc[3] > proposal_number:
            proposal_number = loc[3]
            proposal_value = loc[4]
            
        # Add response to list
        responses.append(loc)
        
        # If we have received 2f+1 responses, we can move on to Phase 2A
        if len(responses) >= QUORUM_AMOUNT:
            break
        
    # Send Phase 2A messages
    phase2A_msg = paxos_encode([1, 2, id, proposal_number, proposal_value])
    s.sendto(phase2A_msg, config['acceptors'])


def learner(config, id):
    r = mcast_receiver(config['learners'])
    s = mcast_sender()
    messages = []
    messages_running = []
    learned = 0

    def learner_timeout(id):
        sleep(TIMEOUT)
        # Reseting the quorum and requesting the results for the instance from the acceptors
        if len(messages[id]) < QUORUM_AMOUNT:
            messages[id] = []
            messages_running[id] = False
            resp = paxos_encode([id+1, 4])
            s.sendto(resp, config['acceptors'])

    # sending a message to get all of the accepted proposals
    update_message = paxos_encode([id, 3])
    s.sendto(update_message, config['learners'])

    while True:
        # TODO: check if id was already processed
        # We receive a message which consists out of id of the message and value of the message
        msg = paxos_decode(r.recv(2**16)) ## list (loc): [size, id, phase, round, value]
        print("Got:", msg)
        # We split message into 2 parts: value and id
        inst_id = int(msg[0]) - 1
        if len(msg) > 1:
            # If we receive un "update message" from another learner
            if msg[1] == 1:
                while inst_id <= len(messages):
                    messages.append([])
                if messages[inst_id] == []:
                    messages[inst_id].append(msg[3])
                    messages[inst_id].append(msg[3])
                    messages[inst_id].append(msg[3])
                    learned+=1

            # If we received PAXOS round 2 message
            if(msg[1] == 2):
                # print("Instance: {} Learned: {}".format(inst_id, msg[3]))
                value = msg[3]
                # If id is > len(messages) we need to extend array of messages up to the needed size,
                # we don't show that we learned anything, bc we've extended array without assigning
                # the message with the smaller id
                if inst_id > len(messages):
                    print("Init mor: {}".format(inst_id))
                    while(inst_id - 1 > len(messages)):
                        messages.append([])
                        messages_running.append(False)
                    messages.append([value])
                    messages_running.append(True)
                # If id is == len(messages) we apppend a message to the array,
                # we don't show that we learned anything if learned isn't equal to msg len
                # because in this case we're still missing message somewhere in the middle
                elif inst_id == len(messages):
                    print("Init eq: {}".format(inst_id))
                    # if learned == len(messages) and len(messages[id]) == QUORUM_AMOUNT - 1:
                    #     if value == messages[id][0]:
                    #         print("Instance: {} Learned: {}".format(id, value))
                    #     learned += 1
                    messages.append([value])
                    messages_running.append(True)

                # If id is < len(messages) we swap [] (value of not received message) with the received value,
                # then if id == learned, then we can print the value as learned until we reach first
                # undefined message
                elif inst_id < len(messages):
                    # print("Init les: {}, msg: {}, learned: {}, quorum: {}".format(inst_id, messages[inst_id], learned, QUORUM_AMOUNT))
                    if len(messages[inst_id]) >= QUORUM_AMOUNT - 1 and inst_id == learned:
                        print("Inside if: {}".format(learned))
                        while(len(messages) > learned and len(messages[learned]) >= QUORUM_AMOUNT - 1 ):
                            validity = True
                            for val in messages[learned]:
                                if val != messages[learned][0]:
                                    validity = False
                                    break
                            if validity:
                                messages[inst_id].append(value)
                                print("Instance: {} Learned: {} Amount:{}".format(learned, messages[learned][0], len(messages[learned])))
                            learned += 1
                    else:
                        messages[inst_id].append(value)
                if len(messages[inst_id]) == 1:
                    messages_running[inst_id] = True
                    thread = Thread(target=learner_timeout, args=[inst_id])
                    thread.start()
                    thread.join()


            # If we received Learner update message, and we need to propagate the data that we know
            if msg[1] == 3:
                inst = 0
                for i in messages:
                    if inst < learned:
                        resp = paxos_encode([inst+1, 1, i[0]])
                        s.sendto(resp, config['learners'])
                        inst += 1
                    else:
                        break
        sys.stdout.flush()


def client(config, id):
    print ('-> client ', id)
    s = mcast_sender()
    inst_id = 1
    for value in sys.stdin:
        value = value.strip()
        print ("client: sending %s to proposers" % (value))
        s.sendto(paxos_encode([inst_id,  0, value]), config['proposers'])
        inst_id += 1
    print ('client done.')


if __name__ == '__main__':
        cfgpath = sys.argv[1]
        config = parse_cfg(cfgpath)
        role = sys.argv[2]
        id = int(sys.argv[3])
        if role == 'acceptor':
            rolefunc = acceptor
        elif role == 'proposer':
            rolefunc = proposer
        elif role == 'learner':
            rolefunc = learner
        elif role == 'client':
            rolefunc = client
        rolefunc(config, id)
