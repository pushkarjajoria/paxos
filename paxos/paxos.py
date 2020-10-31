#!/usr/bin/env python3.6
import json
import sys
import socket
import struct

"""------------------------------"""
from enum import Enum
import json
import datetime
import time
import random
from threading import Thread


class MessageType:
    CLIENT_MSG = 1
    PHASE1A = 2
    PHASE2A = 3
    PHASE1B = 4
    PHASE2B = 5
    PHASE3 = 6


class DictIds:
    INSTANCE_ID = 'instance_ID'
    VALUE = 'value'
    C_RND = 'c_rnd'
    V_RND = 'v_rnd'
    C_VAL = 'c_val'
    V_VAL = 'v_val'
    RND = 'rnd'
    MSG_TYPE = 'msg_type'
    STATUS = "status"
    TIME = "time"


class MsgHandler:

    def __init__(self, config):
        self.config = config

    def create_phase1A_msg(self, c_rnd, instance_id):
        json_dict = {
            DictIds.MSG_TYPE: MessageType.PHASE1A,
            DictIds.INSTANCE_ID: instance_id,
            DictIds.C_RND: c_rnd
        }
        json_dump = json.dumps(json_dict).encode("utf_8")
        return json_dump

    def create_phase1B_msg(self, rnd, v_rnd, v_val, instance_id):
        json_dict = {
            DictIds.MSG_TYPE: MessageType.PHASE1B,
            DictIds.INSTANCE_ID: instance_id,
            DictIds.RND: rnd,
            DictIds.V_RND: v_rnd,
            DictIds.V_VAL: v_val,
        }
        json_dump = json.dumps(json_dict).encode("utf_8")
        return json_dump

    def create_phase2A_msg(self, c_rnd, c_val, instance_id):
        json_dict = {
            DictIds.MSG_TYPE: MessageType.PHASE2A,
            DictIds.INSTANCE_ID: instance_id,
            DictIds.C_RND: c_rnd,
            DictIds.C_VAL: c_val
        }
        json_dump = json.dumps(json_dict).encode("utf_8")
        return json_dump

    def create_phase2B_msg(self, v_rnd, v_val, instance_id):
        json_dict = {
            DictIds.MSG_TYPE: MessageType.PHASE2B,
            DictIds.INSTANCE_ID: instance_id,
            DictIds.V_RND: v_rnd,
            DictIds.V_VAL: v_val
        }
        json_dump = json.dumps(json_dict).encode("utf_8")
        return json_dump

    def create_phase3_msg(self, v_val, instance_id):
        json_dict = {
            DictIds.MSG_TYPE: MessageType.PHASE3,
            DictIds.INSTANCE_ID: instance_id,
            DictIds.V_VAL: v_val
        }
        json_dump = json.dumps(json_dict).encode("utf_8")
        return json_dump

    def create_proposer_msg(self, value):
        json_dict = {
            DictIds.MSG_TYPE: MessageType.CLIENT_MSG,
            DictIds.VALUE: value
        }
        json_dump = json.dumps(json_dict).encode("utf_8")
        return json_dump


class Learner:
    def __init__(self):
        self.learned_values = {}

    def handle_phase3(self, msg_dict):
        instance_id = msg_dict[DictIds.INSTANCE_ID]
        if instance_id in self.learned_values:
            pass
        else:
            print(msg_dict[DictIds.V_VAL])
            sys.stdout.flush()
            self.learned_values[instance_id] = msg_dict[DictIds.V_VAL]

    def read_json_str(self, network_msg):
        json_dict = {}
        return json_dict


class Acceptor:

    def __init__(self, id, s):
        self.acceptor_id = id
        self.sender = s
        self.v_rnd = {}
        self.v_val = {}
        self.rnd = {}

    def handle_phase1a(self, msg):
        instance_id = msg[DictIds.INSTANCE_ID]
        rnd = self.rnd.get(instance_id, 0)
        if msg[DictIds.C_RND] > rnd:
            self.rnd[instance_id] = msg[DictIds.C_RND]
            rnd = self.rnd[instance_id]
            v_rnd = self.v_rnd.get(instance_id, 0)
            v_val = self.v_val.get(instance_id, float('-inf'))
            self.execute_phase1b(rnd, v_rnd, v_val, instance_id)
        else:
            pass
            # print("Ignoring Phase1A message")

    def execute_phase1b(self, rnd, v_rnd, v_val, instance_id):
        phase1b_msg = msg_handler.create_phase1B_msg(rnd, v_rnd, v_val, instance_id)
        self.sender.sendto(phase1b_msg, msg_handler.config['proposers'])

    def execute_phase2b(self, v_rnd, v_val, instance_id):
        phase2b_msg = msg_handler.create_phase2B_msg(v_rnd, v_val, instance_id)
        self.sender.sendto(phase2b_msg, msg_handler.config['proposers'])

    def handle_phase2a(self, msg_dict):
        instance_id = msg_dict[DictIds.INSTANCE_ID]
        c_rnd = msg_dict[DictIds.C_RND]
        c_val = msg_dict[DictIds.C_VAL]
        if c_rnd > 1:
            print("C-RND is greater than 1 with msg dict" + str(msg_dict))
        rnd = self.rnd.get(instance_id, 0)
        if msg_dict[DictIds.C_RND] >= rnd:
            self.v_rnd[instance_id] = c_rnd
            self.v_val[instance_id] = c_val
            self.execute_phase2b(self.v_rnd[instance_id], self.v_val[instance_id], instance_id)
        else:
            pass
            # print("Ignoring Phase1A message")


class Proposer(Thread):
    class ProposerStatus(Enum):
        IDLE = 1
        E_PHASE1A = 2
        W_PHASE1B = 3
        E_PHASE2A = 4
        W_PHASE2B = 5
        HALTED = 6

    def __init__(self, id, s):
        Thread.__init__(self)
        self.num_of_proposers = 2
        self.status = {}
        self.instance_id = -1
        self.proposer_id = id
        self.client_val = {}
        self.c_val = {}
        self.c_rnd = {}
        self.sender = s
        self.quorum_2a = {}
        self.quorum_3 = {}
        self.required_quorum = 1
        self.max_v_rnd_v_val = {}
        self.RETRY_THRESHOLD = 20  # seconds
        self.buffer = []
        self.msg_counter = 0
        self.twoa_counter = 0
        self.phase3_counter = 0
        self.phase1_counter = 0
        self.quorun_not_done_2A = {}
        self.quorun_not_done_3 = {}

    def oracle_am_i_leader(self):
        return self.proposer_id == 1

    def retry_waiting_instances(self):
        for instance_id in self.status.keys():
            cached_status = self.status[instance_id]
            status, start_time = cached_status[DictIds.STATUS], cached_status[DictIds.TIME]
            current_time = datetime.datetime.now()
            if current_time - start_time > datetime.timedelta(
                    seconds=self.RETRY_THRESHOLD) and status == self.ProposerStatus.W_PHASE1B:
                self.status[instance_id] = {
                    DictIds.STATUS: self.ProposerStatus.W_PHASE1B,
                    DictIds.TIME: datetime.datetime.now()
                }
                self.execute_phase1a(instance_id)

    def execute_phase1a(self, instance_id):
        if instance_id in self.c_rnd:
            self.c_rnd[instance_id] += self.num_of_proposers
            if self.c_rnd[instance_id] >= 10 and not self.oracle_am_i_leader():
                self.status[instance_id] = {
                    DictIds.STATUS: self.ProposerStatus.HALTED,
                    DictIds.TIME: datetime.datetime.now()
                }
        else:
            self.c_rnd[instance_id] = self.proposer_id
        msg = msg_handler.create_phase1A_msg(self.c_rnd[instance_id], instance_id)
        self.phase1_counter += 1
        print("1A_counter -> ", self.phase1_counter)
        self.sender.sendto(msg, msg_handler.config['acceptors'])
        self.status[instance_id] = {
            DictIds.STATUS: self.ProposerStatus.W_PHASE1B,
            DictIds.TIME: datetime.datetime.now()
        }

    def execute_phase2a(self, c_rnd, c_val, instance_id):
        self.twoa_counter += 1
        print("2A_counter -> ", self.twoa_counter)
        msg = msg_handler.create_phase2A_msg(c_rnd, c_val, instance_id)
        self.sender.sendto(msg, msg_handler.config['acceptors'])

    def handle_phase1b(self, msg_dict):
        instance_id = msg_dict[DictIds.INSTANCE_ID]
        self.status[self.instance_id] = {
            DictIds.STATUS: self.ProposerStatus.E_PHASE2A,
            DictIds.TIME: datetime.datetime.now()
        }
        self.quorum_2a[instance_id] = self.quorum_2a.get(instance_id, 0) + 1
        max_v_rnd, max_v_val = self.max_v_rnd_v_val.get(instance_id, (0, float('-inf')))
        if max_v_rnd < msg_dict[DictIds.V_RND]:
            max_v_rnd, max_v_val = msg_dict[DictIds.V_RND], msg_dict[DictIds.V_VAL]
            self.max_v_rnd_v_val[instance_id] = (max_v_rnd, max_v_val)
        if self.quorum_2a[instance_id] >= self.required_quorum and self.quorun_not_done_2A.get(instance_id, True):
            self.quorun_not_done_2A[instance_id] = False
            if max_v_rnd == 0:
                self.c_val[instance_id] = self.client_val[instance_id]
            else:
                self.c_val[instance_id] = max_v_val
            c_rnd, c_val = self.c_rnd[instance_id], self.c_val[instance_id]
            self.execute_phase2a(c_rnd, c_val, instance_id)

    def handle_phase2b(self, msg_dict):
        instance_id = msg_dict[DictIds.INSTANCE_ID]
        self.quorum_3[instance_id] = self.quorum_3.get(instance_id, 0) + 1
        if msg_dict[DictIds.V_RND] != self.c_rnd[instance_id]:
            return
        if self.quorum_2a[msg_dict[DictIds.INSTANCE_ID]] >= self.required_quorum and self.quorun_not_done_3.get(instance_id, True):
            self.quorun_not_done_3[instance_id] = False
            v_val = msg_dict[DictIds.V_VAL]
            self.execute_phase3(v_val, instance_id)

    def execute_phase3(self, v_val, instance_id):
        self.phase3_counter += 1
        print("3A_counter -> ", self.phase3_counter)
        msg = msg_handler.create_phase3_msg(v_val, instance_id)
        self.sender.sendto(msg, msg_handler.config['learners'])

    def handle_client_msg(self, msg):
        self.instance_id += 1
        self.status[self.instance_id] = {
            DictIds.STATUS: self.ProposerStatus.E_PHASE1A,
            DictIds.TIME: datetime.datetime.now()
        }
        self.client_val[self.instance_id] = msg[DictIds.VALUE]
        self.execute_phase1a(self.instance_id)
        pass

    def run(self):
        r = mcast_receiver(config['proposers'])
        while True:
            msg = r.recv(2 ** 16).decode("utf_8")
            msg_dict = json.loads(msg)
            msg_type = int(msg_dict['msg_type'])
            if msg_type == MessageType.CLIENT_MSG:
                self.msg_counter += 1
            self.buffer.append(msg)
        # Read values and add them to the buffer


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
        msg = r.recv(2 ** 16).decode("utf_8")
        # fake acceptor! just forwards messages to the learner
        msg_dict = json.loads(msg)
        # TODO: Try catch
        msg_type = int(msg_dict[DictIds.MSG_TYPE])
        if msg_type == MessageType.PHASE1A:
            _acceptor.handle_phase1a(msg_dict)
        elif msg_type == MessageType.PHASE2A:
            _acceptor.handle_phase2a(msg_dict)
        else:
            # print("proposer: sending %s to acceptors" % (msg)
            raise Exception("Unknown Message message = [" + msg + "]")


def proposer(config, id):
    s = mcast_sender()
    _proposer = Proposer(id, s)
    _proposer.start()
    # leader = _proposer.oracle_am_i_leader()
    print('-> proposer', id)
    time.sleep(2)
    while True:
        try:
            msg = _proposer.buffer.pop()
        except IndexError as e:
            continue
        # Check msg type
        # Can either be Client Msg, P1B or P2B
        # Execute P1A, P2A, P3
        # TODO: Check for the msg headers
        msg_dict = json.loads(msg)
        # TODO: Try catch
        msg_type = int(msg_dict['msg_type'])
        if msg_type == MessageType.CLIENT_MSG:
            print(f"Total Messages processed till now = {_proposer.msg_counter}")
            _proposer.handle_client_msg(msg_dict)
        elif msg_type == MessageType.PHASE1B:
            _proposer.handle_phase1b(msg_dict)
        elif msg_type == MessageType.PHASE2B:
            _proposer.handle_phase2b(msg_dict)
        else:
            # print("proposer: sending %s to acceptors" % (msg)
            raise Exception("Unknown Message message = [" + msg + "]")
        # _proposer.retry_waiting_instances()


def learner(config, id):
    _learner = Learner()
    r = mcast_receiver(config['learners'])
    while True:
        msg = r.recv(2 ** 16).decode("utf_8")
        msg_dict = json.loads(msg)
        msg_type = int(msg_dict[DictIds.MSG_TYPE])
        if msg_type == MessageType.PHASE3:
            _learner.handle_phase3(msg_dict)
        else:
            raise Exception("Unknown Message message = [" + str(msg_dict) + "]")


def client(config, id):
    print('-> client ', id)
    s = mcast_sender()
    values = sys.stdin
    # values = ["1", "2", "4"]
    for i, value in enumerate(values):
        value = value.strip()
        print("client: sending %s to proposers" % value)
        json_msg = msg_handler.create_proposer_msg(value)
        s.sendto(json_msg, config['proposers'])
        if i % 2000 == 0 and i > 0:
            time.sleep(0.5)

    print('client done.')


if __name__ == '__main__':
    cfgpath = sys.argv[1]
    # cfgpath = "paxos.conf"
    config = parse_cfg(cfgpath)
    msg_handler = MsgHandler(config)

    role = sys.argv[2]
    # role = 'acceptor'
    # id = 1
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
