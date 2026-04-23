from enum import Enum
from fintekkers.models.security.identifier.identifier_pb2 import IdentifierProto

class ReconResultStatus(Enum):
    RECON_GOOD=1
    RECON_UNKNOWN=0
    RECON_BAD=-1

class ReconResult():
    def __init__(self, result:ReconResultStatus, security_id:IdentifierProto, message:str, transaction_created=True):
        self.result = result
        self.message = message
        self.security_id = security_id
        self.transaction_created = transaction_created