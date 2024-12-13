import datetime
from jose import JWTError, jwt

from sqlalchemy.orm import Session
from typing import Optional

from . import models, schemas
from app.utils import get_password_hash, verify_password

from .config import ALGORITHM, SECRET_KEY


def get_user(db: Session, user_id: int):
    return db.query(models.ApiUser).filter(models.ApiUser.id == user_id).first()


def get_user_by_email(db: Session, email: str):
    return db.query(models.ApiUser).filter(models.ApiUser.email == email).first()


def get_user_by_username(db: Session, username: str):
    return db.query(
        models.ApiUser).filter(models.ApiUser.username == username).first()


def get_users(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.ApiUser).offset(skip).limit(limit).all()


def create_user(db: Session, user: schemas.ApiUserCreate):
    hashed_password = get_password_hash(user.plain_password)
    userdict = user.dict()
    userdict.pop('plain_password')
    db_user = models.ApiUser(**userdict, hashed_password=hashed_password)
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return db_user


def authenticate_user(db: Session, username: str, password: str):
    user = get_user_by_username(db, username)
    if not user:
        return False
    if not verify_password(password, user.hashed_password):
        return False
    return user


def create_access_token(data: dict, expires_delta: Optional[datetime.timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.datetime.utcnow() + expires_delta
    else:
        expire = datetime.datetime.utcnow() + datetime.timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

