from pydantic import BaseModel
from datetime import datetime
from typing import Optional

class UserType(BaseModel):
    user_type_id: int
    user_type_desc: str

class UserAccount(BaseModel):
    user_id: int
    password_hash: str
    name: str
    email: str
    user_type_id: int

class HarborDetail(BaseModel):
    harbor_id: int
    harbor_name: str
    harbor_address: str

class HarborGuardUser(BaseModel):
    hg_user_id: int
    harbor_id: int
    user_id: int

class DryLeaves(BaseModel):
    dry_leaves_ID: int
    dry_weight: float
    dry_date: datetime
    dry_image: Optional[str]

class WetLeaves(BaseModel):
    wet_leaves_ID: int
    wet_weight: float
    wet_date: datetime
    wet_image: Optional[str]

class PowderedLeaves(BaseModel):
    powdered_leaves_ID: int
    powdered_weight: float
    powdered_date: datetime
    powdered_image: Optional[str]

class BatchInformation(BaseModel):
    batch_ID: int
    batch_date: datetime
    dry_leaves_ID: Optional[int]
    wet_leaves_ID: Optional[int]
    powdered_leaves_ID: Optional[int]
    status: int
    dry_leaves: Optional[DryLeaves]
    wet_leaves: Optional[WetLeaves]
    powdered_leaves: Optional[PowderedLeaves]

class HarborCheckpoint(BaseModel):
    checkpoint_ID: int
    harbor_batch_rescale: Optional[float] = None
    sent_date: datetime
    arrival_date: Optional[datetime] = None
    transport_status: Optional[int] = None
    batch_ID: int
    hg_user_ID: Optional[int] = None
    harbor_ID: int

class HarborCheckpointInformation(BaseModel):
    checkpoint_ID: int
    harbor_batch_rescale: float
    sent_date: datetime
    arrival_date: datetime
    transport_status: str
    batch_ID: int
    batch_date: datetime
    dry_leaves_ID: Optional[int]
    wet_leaves_ID: Optional[int]
    powdered_leaves_ID: Optional[int]
    status: int
    dry_leaves: Optional[DryLeaves]
    wet_leaves: Optional[WetLeaves]
    powdered_leaves: Optional[PowderedLeaves]
    hg_user_ID: int

class HarborUpdateModel(BaseModel):
    harbor_batch_rescale: Optional[float] = None
    arrival_date: Optional[datetime] = None
    hg_user_ID: Optional[int] = None