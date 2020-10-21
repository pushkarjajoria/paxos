from enum import Enum


class MessageType(Enum):
    CLIENT_MSG = 1
    PHASE1A = 2
    PHASE2A = 3
    PHASE1B = 4
    PHASE2B = 5


class Proposer:

    def __init__(self):
        self.buff = []
        pass

    def handle_phase1a(self):
        pass

    def handle_phase2a(self):
        pass

    def handle_phase1b(self):
        pass

    def handle_phase2b(self):
        pass

    def handle_phase3(self):
        pass

    def handle_client_msg(self):
        pass

