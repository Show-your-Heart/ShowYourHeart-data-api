from typing import Optional, List
from pydantic import BaseModel


class Token(BaseModel):
    access_token: str
    token_type: str


class TokenData(BaseModel):
    username: Optional[str] = None


class ApiUserBase(BaseModel):
    email: str
    username: str
    fullname: Optional[str] = None


class ApiUserCreate(ApiUserBase):
    plain_password: str


class ApiUser(ApiUserBase):
    id: int
    is_active: bool

    class Config:
        orm_mode = True

