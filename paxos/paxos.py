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

class DictIds(Enum):
    INSTANCE_ID = 'instance_ID'
    VALUE = 'value'
    C_RND = 'c_rnd'
    V_RND = 'v_rnd'
    C_VAL = 'c_val'
    V_VAL = 'v_val'


class MsgHandler():

    def __init__(self, config):
        self.config = config

    def create_phase1A_msg(self, c_rnd, instance_id):
        # TODO: JSON
        return "JSON_MSG"

    def create_phase1B_msg(self, rnd, v_rnd, v_val, instance_id):
        # TODO: JSON
        return "JSON_MSG"


class NetworkWorker:
    def __init__(self):
        pass

    def read_json_str(self, msg):
        json_dict = {}
        return json_dict


class Acceptor(NetworkWorker):

    def __init__(self, id, s):
        self.acceptor_id = id
        self.sender = s
        self.v_rnd = {}
        self.v_val = {}
        self.rnd = {}

    def handle_phase1a(self, msg):
        rnd = self.rnd.get(msg[DictIds.INSTANCE_ID], 0)
        if msg[DictIds.C_RND] > rnd:
            self.rnd[msg[DictIds.INSTANCE_ID]] = msg[DictIds.C_RND]
            rnd = self.rnd[msg[DictIds.INSTANCE_ID]]
            v_rnd = self.v_rnd[msg[DictIds.INSTANCE_ID]]
            v_val = self.v_rnd[msg[DictIds.INSTANCE_ID]]
            self.execute_phase1b(msg, rnd, v_rnd, v_val)
        else:
            print("Ignoring Phase1A message")

    def execute_phase1b(self, rnd, v_rnd, v_val):
        phase1b_msg = msg_handler.create_phase1B_msg(rnd, v_rnd, v_val)
        self.sender(phase1b_msg)

    def handle_phase2a(self, msg):
        rnd = self.rnd.get(msg[DictIds.INSTANCE_ID], 0)
        if msg[DictIds.C_RND] > rnd:
            rnd = self.rnd[msg[DictIds.INSTANCE_ID]]
            v_rnd = self.v_rnd[msg[DictIds.C_RND]]
            v_val = self.v_rnd[msg[DictIds.C_VAL]]
            self.execute_phase1b(msg, rnd, v_rnd, v_val)
        else:
            print("Ignoring Phase1A message")

        pass


class Client(NetworkWorker):
    def __init__(self):
        pass

    def create_proposer_request(self, value):
        msg_type = MessageType.CLIENT_MSG
        proposer_req = ""   # This should be JSON
        return proposer_req


class Proposer(NetworkWorker):

    def __init__(self, id, s):
        self.instance_id = -1
        self.proposer_id = id
        self.buff = []
        self.c_val = {}
        self.c_rnd = {}
        self.sender = s
        pass

    def execute_phase1a(self):
        if self.instance_id in self.c_rnd:
            self.c_rnd[self.instance_id] += 1
        else:
            self.c_rnd[self.instance_id] = 0
        msg = msg_handler.create_phase1A_msg(self.c_rnd[self.instance_id], self.instance_id)
        self.sender.sendto(msg, msg_handler.config['acceptors'])

    def execute_phase2a(self):
        pass

    def handle_phase1b(self):
        self.execute_phase2a()
        pass

    def handle_phase2b(self):
        self.handle_phase3()
        pass

    def execute_phase3(self):
        pass

    def handle_client_msg(self, msg):
        self.instance_id += 1
        self.execute_phase1a()
        pass

"""----------------------------------"""


global msg_handler

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
    _acceptor = Acceptor(id, s)
    while True:
        msg = r.recv(2 ** 16)
        # fake acceptor! just forwards messages to the learner
        msg_dict = _acceptor.read_json_str()
        # TODO: Try catch
        msg_type = int(msg_dict['msg_type'])
        msg_instance_id = msg_dict["instance_id"]
        if msg_type == MessageType.PHASE1A:
            _acceptor.handle_phase1a(msg_dict)
        elif msg_type == MessageType.PHASE2A:
            _acceptor.handle_phase2a()
        else:
            # print("proposer: sending %s to acceptors" % (msg)
            raise Exception("Unknown Message message = [" + msg + "]")
        if id == 1:
            # print("acceptor: sending %s to learners" % (msg)
            s.sendto(msg, config['learners'])


def proposer(config, id):
    s = mcast_sender()
    _proposer = Proposer(id, s)
    print('-> proposer', id)
    r = mcast_receiver(config['proposers'])
    while True:
        msg = r.recv(2 ** 16)
        # check msg type
        # Can either be Client Msg, P1B or P2B
        # Execute P1A, P2A, P3
        # TODO: Check for the msg headers
        msg_dict = _proposer.read_json_str(msg)
        # TODO: Try catch
        msg_type = int(msg_dict['msg_type'])
        if msg_type == MessageType.CLIENT_MSG:
            _proposer.handle_client_msg(msg)
        elif msg_type == MessageType.PHASE1B:
            _proposer.handle_phase1b()
        elif msg_type == MessageType.PHASE2B:
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
    for value in values:
        value = value.strip()
        print("client: sending %s to proposers" % (value))
        # Add msg type here
        data_set = {}
        json_msg = _client.create_proposer_request(value)
        s.sendto(json_msg, config['proposers'])
    print('client done.')


if __name__ == '__main__':
    if len(sys.argv) <= 1:
        cfgpath = "../paxos.conf"
    else:
        cfgpath = sys.argv[1]
    config = parse_cfg(cfgpath)
    msg_handler = MsgHandler(config)

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
