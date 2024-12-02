import ast
import uuid
from datetime import datetime
from sqlalchemy import Boolean, Column, Integer, String
import requests
from pandas import DataFrame
import json
import time

from .database import Base

import os

from dotenv import load_dotenv
load_dotenv(dotenv_path='.env.devel', override=True)




class ApiUser(Base):
    __tablename__ = "apiuser"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True)
    username = Column(String, unique=True, nullable=False)
    fullname = Column(String, nullable=True)
    hashed_password = Column(String, nullable=False)
    is_active = Column(Boolean, default=True)

