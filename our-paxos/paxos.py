#!/usr/bin/env python3
import sys
import socket
import struct

ACCEPTORS_AMOUNT = 3
QUORUM_AMOUNT = ACCEPTORS_AMOUNT/2 + 1

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


# ----------------------------------------------------

### Message structure: ###
# every chunk <..> is 2 bytes = int range: 0-65535
# 
# <instance_number><phase_ID>PHASE_PAYLOAD
# 
# phase ID is:
# 1: phase 1A when receiving, 1B when sending
# 2: phase 2A when receiving, 2B when sending
# 
# PHASE_PAYLOAD is:
# 1A: <c-rnd>              total = 3 chunks
# 1B: <rnd><v-rnd><v-val>  total = 5 chunks
# 2A: <c-rnd><c-val>       total = 4 chunks
# 2B: <v-rnd><v-val>       total = 4 chunks
#


def paxos_encode(loc):
    """Encode a paxos message into binary, to be sent through a network
    socket.

    loc -- list of chunks, a list of integers containing Paxos' phase 
    info, including consensus instance number
    """
    msg = nbytes = 0

    for elem in loc:
        ## supporting integers only
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



def acceptor(config, id):
    print ('-> acceptor', id)
    # dictionary of acceptor states where
    # { paxos_instance: state }
    paxos_instances = {} 
    
    r = mcast_receiver(config['acceptors'])
    s = mcast_sender()
    while True:
        # init
        init_state = {"rnd": 0, "v-rnd": 0, "v-val": 0} # acceptor state
        # recv to a large buffer
        msg = r.recv(2**16)
        # extract the instance and phase
        loc = paxos_decode(msg)
        instance = loc[0]
        phase = loc[1]
        
        # set the initial state if it's the first time we start
        # this paxos instance
        if instance not in paxos_instances.keys():
            paxos_instances[instance] = init_state

        # let's change the current state
        state = paxos_instances[instance]
        if phase == 1:
            # received phase 1A msg from proposer
            if loc[2] > state['rnd']:
                # update state and paxos_
                state['rnd'] = loc[2]
                print("i: {} p: {}, state: {}".format(instance, phase, state))
                # encode and send phase 1B to proposer
                msg = paxos_encode([instance, phase, state['rnd'], state['v-rnd'], state['v-val']])
                s.sendto(msg, config['proposers'])
        elif phase == 2:
            # received phase 2A msg from proposer
            # loc[2] = c-rnd, loc[3] = c-val
            if loc[2] >= state['rnd']:
                #state['rnd'] = loc[2] # not in the slides, but it makes sense (?)
                state['v-rnd'] = loc[2]
                state['v-val'] = loc[3]
                print("i: {} p: {}, state: {}".format(instance, phase, state))

                # encode and send phase 2B to proposer
                msg = paxos_encode([instance, phase, state['v-rnd'], state['v-val']])
                s.sendto(msg, config['learners'])
        else:
            print('Wrong message received: unknown phase. loc: {}'.format(loc))


def proposer(config, id):
    print ('-> proposer', id)
    r = mcast_receiver(config['proposers'])
    s = mcast_sender()

    # dummy proposer for testing
    for i in range(10):
        # send phase 1A msg
        msg1 = paxos_encode([i,1,3])
        s.sendto(msg1, config['acceptors'])
        # send phase 2A msg
        msg2 = paxos_encode([i,2,32,55])
        s.sendto(msg2, config['acceptors'])

    while True:
        msg = r.recv(2**16)
        # fake proposer! just forwards message to the acceptor
        #if id == 1:
            # print "proposer: sending %s to acceptors" % (msg)

            # test phase 1a
            #msg = paxos_encode([1, 2])
            #s.sendto(msg, config['acceptors'])
            


def learner(config, id):
    r = mcast_receiver(config['learners'])
    s = mcast_sender()
    messages = []
    learned = 0

    # sending a message to get all of the accepted proposals
    update_message = paxos_encode([id, 3])
    s.sendto(update_message, config['learners'])

    while True:
        # TODO: check if id was already processed
        # We receive a message which consists out of id of the message and value of the message
        msg = paxos_decode(r.recv(2**16)) ## list (loc): [size, id, phase, round, value]
        print("Got:", msg)
        # We split message into 2 parts: value and id
        inst_id = msg[0]
        # If we received PAXOS round 2 message
        if(msg[2] == 2):
            value = msg[3]
            # If id is > len(msg) we need to extend array of messages up to the needed size,
            # we don't show that we learned anything, bc we've extended array without assigning
            # the message with the smaller id
            if inst_id > len(messages):
                while(inst_id - 1 > len(messages)):
                    messages.append([])
                messages.append([value])
            # If id is == len(msg) we apppend a message to the array,
            # we don't show that we learned anything if learned isn't equal to msg len
            # because in this case we're still missing message somewhere in the middle
            elif inst_id == len(messages):
                # if learned == len(messages) and len(messages[id]) == QUORUM_AMOUNT - 1:
                #     if value == messages[id][0]:
                #         print("Instance: {} Learned: {}".format(id, value))
                #     learned += 1
                messages.append([value])

            # If id is < len(msg) we swap -1 (value of not received message) with the received value,
            # then if id == learned, then we can print the value as learned until we reach first
            # undefined message
            elif inst_id < len(messages):
                if len(messages[inst_id]) == QUORUM_AMOUNT - 1 and inst_id == learned:
                    while(len(messages[learned]) >= QUORUM_AMOUNT and len(messages) > learned):
                        validity = True
                        for val in messages[learned]:
                            if val != messages[learned][0]:
                                validity = False
                                break
                        if validity:
                            print("Instance: {} Learned: {}".format(learned, messages[learned][0]))
                        learned += 1
                messages[inst_id].append(value)
            resp = paxos_encode([inst_id, 3, value])
            s.sendto(resp, config['acceptors'])
        # If we received Learner update message

        if msg[2] == 3:
            inst = 0
            for i in messages:
                resp = paxos_encode([inst, 1, i])
                s.sendto(resp, config['learners'])
                inst += 1
        if msg[2] == 1:
            while inst_id <= len(messages):
                messages.append(-1)
            if messages[inst_id] == -1:
                inst_id[inst_id] = msg[3]

        sys.stdout.flush()


def client(config, id):
    print ('-> client ', id)
    s = mcast_sender()
    for value in sys.stdin:
        value = value.strip()
        print ("client: sending %s to proposers" % (value))
        s.sendto(value.encode(), config['proposers'])
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
