#!/usr/bin/env python
import json
import sys
import socket
import struct

"""------------------------------"""
from enum import Enum


class MessageType(Enum):
    CLIENT_MSG = 1
    PHASE1A = 2
    PHASE2A = 3
    PHASE1B = 4
    PHASE2B = 5


class NetworkWorker:
    def __init__(self):
        pass

    def read_json_str(self, msg):
        json_dict = {}
        return json_dict




class Client(NetworkWorker):
    def __init__(self):
        pass

    def create_proposer_request(self, value, instance_id):
        proposer_req = ""   # This should be JSON
        return proposer_req


class Proposer(NetworkWorker):

    def __init__(self, id):
        self.proposer_id = id
        self.buff = []
        pass

    def handle_phase1a(self):
        pass

    def handle_phase2a(self):
        pass

    def handle_phase1b(self):
        self.handle_phase2a()
        pass

    def handle_phase2b(self):
        self.handle_phase3()
        pass

    def handle_phase3(self):
        pass

    def handle_client_msg(self, msg):
        self.handle_phase1a()
        pass

"""----------------------------------"""


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
    print('-> acceptor', id)
    state = {}
    r = mcast_receiver(config['acceptors'])
    s = mcast_sender()
    while True:
        msg = r.recv(2 ** 16)
        # fake acceptor! just forwards messages to the learner
        if id == 1:
            # print("acceptor: sending %s to learners" % (msg)
            s.sendto(msg, config['learners'])


def proposer(config, id):
    _proposer = Proposer(id)
    print('-> proposer', id)
    r = mcast_receiver(config['proposers'])
    s = mcast_sender()
    while True:
        msg = r.recv(2 ** 16)
        # check msg type
        # Can either be Client Msg, P1B or P2B
        # Execute P1A, P2A, P3
        msg_dict = _proposer.read_json_str()
        # TODO: Try catch
        msg_type = int(msg_dict['msg_type'])
        if msg_type == MessageType.CLIENT_MSG:
            _proposer.handle_client_msg(msg)
        elif msg_type == MessageType.PHASE1B:
            _proposer.handle_phase1b()
        elif msg_type == MessageType.PHASE1B:
            _proposer.handle_phase2b()
        else:
            # print("proposer: sending %s to acceptors" % (msg)
            raise Exception("Unknown Message message = [" + msg + "]")


def learner(config, id):
    r = mcast_receiver(config['learners'])
    while True:
        msg = r.recv(2 ** 16)
        print(msg)
        sys.stdout.flush()


def client(config, id):
    _client = Client()
    print('-> client ', id)
    s = mcast_sender()
    # values = sys.stdin
    values = ["1", "2", "4"]
    for instance_id, value in enumerate(values):
        value = value.strip()
        print("client: sending %s to proposers" % (value))
        # Add msg type here
        data_set = {}
        json_msg = _client.create_proposer_request(value, instance_id)
        s.sendto(json_msg, config['proposers'])
    print('client done.')


if __name__ == '__main__':
    if len(sys.argv) <= 1:
        cfgpath = "../paxos.conf"
    else:
        cfgpath = sys.argv[1]
    config = parse_cfg(cfgpath)
    # role = sys.argv[2]
    role = 'client'
    id = 1
    # id = int(sys.argv[3])
    if role == 'acceptor':
        rolefunc = acceptor
    elif role == 'proposer':
        rolefunc = proposer
    elif role == 'learner':
        rolefunc = learner
    elif role == 'client':
        rolefunc = client
    rolefunc(config, id)
