#!/usr/bin/env python3
import sys
import socket
import struct

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

def acceptor(config, id):
    print ('-> acceptor', id)
    state = {}
    r = mcast_receiver(config['acceptors'])
    s = mcast_sender()
    while True:
        msg = r.recv(2**16)
        # fake acceptor! just forwards messages to the learner
        if id == 1:
            # print "acceptor: sending %s to learners" % (msg)
            s.sendto(msg, config['learners'])


def proposer(config, id):
    print ('-> proposer', id)
    r = mcast_receiver(config['proposers'])
    s = mcast_sender()
    while True:
        msg = r.recv(2**16)
        # fake proposer! just forwards message to the acceptor
        if id == 1:
            # print "proposer: sending %s to acceptors" % (msg)
            s.sendto(msg, config['acceptors'])


def learner(config, id):
    r = mcast_receiver(config['learners'])
    messages = []
    learned = 0
    while True:
        # We receive a message which consists out of id of the message and value of the message
        msg = r.recv(2**32)
        print("Got:", msg)
        # We split message into 2 parts: value and id
        id = int(msg[0:2**16])
        value = msg[2**16:]
        # If id is > len(msg) we need to extend array of messages up to the needed size,
        # we don't show that we learned anything, bc we've extended array without assigning
        # the message with the smaller id
        if id > len(msg):
            while(id - 1 > len(msg)):
                messages.append(-1)
            messages.append(value)
        # If id is = len(msg) we apppend a message to the array,
        # we don't show that we learned anything if learned isn't equal to msg len
        # because in this case we're still missing message somewhere in the middle
        elif id == len(msg):
            if learned == len(msg):
                print("Learned: ", value)
                learned+=1
            messages.append(value)

        # If id is < len(msg) we swap -1 (value of not received message) with the received value,
        # then if id == learned, then we can print the value as learned until we reach first
        # undefined message
        elif id < len(msg):
            messages[id] = value
            if id == learned:
                while(messages[learned] != -1 and len(messages) > learned):
                    print("Learned: ", value)
                    learned += 1

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
