from typing import List

import datetime

from fastapi import Depends, FastAPI, HTTPException, Header, status, Query
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from jose import JWTError, jwt

from sqlalchemy.orm import Session

from . import crud, models, schemas
from .database import SessionLocal, engine, Base

from .config import SECRET_KEY, ALGORITHM, ACCESS_TOKEN_EXPIRE_MINUTES

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# TODO use alembic
Base.metadata.create_all(bind=engine)

api_description = '''
API Data Show Your Heart
'''

api_metainfo = {
    'title': 'Show Your Heart Data',
    'description': api_description,
    'version': '1.0.0',
    'contact': {
     },
    'license_info': {
        "name": "GPL 3.0",
        "url": "https://www.gnu.org/licenses/gpl-3.0.ca.html",
    },
}

tags_metadata = [
    {
        "name": "auth",
        "description": "Token operations. Get an oauth2 token here with your user credentials.",
    },
    {
        "name": "users",
        "description": "Operations with users. The **login** logic is also here until you can request a token.",
    },
    {
        "name": "Data",
        "description": "Data operations",
    },
]

app = FastAPI(openapi_tags=tags_metadata, **api_metainfo)

# allow for CORS
origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_current_user(db: Session = Depends(get_db), token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = schemas.TokenData(username=username)
    except JWTError:
        raise credentials_exception
    user = crud.get_user_by_username(db, username=token_data.username)
    if user is None:
        raise credentials_exception
    return user


def get_current_active_user(current_user: schemas.ApiUser = Depends(get_current_user)):
    if not current_user.is_active:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Inactive user")
    return current_user


@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse(url='/apidata/docs')


@app.post("/token", tags=["auth"], response_model=schemas.Token)
def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends(),
                           db: Session = Depends(get_db)):
    user = crud.authenticate_user(db, form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = datetime.timedelta(minutes=int(ACCESS_TOKEN_EXPIRE_MINUTES))
    access_token = crud.create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires)
    return {"access_token": access_token, "token_type": "bearer"}


@app.post("/users/", tags=["users"], response_model=schemas.ApiUser)
def create_user(user: schemas.ApiUserCreate, db: Session = Depends(get_db)
                , current_user: schemas.ApiUser = Depends(get_current_active_user)):
    db_user = crud.get_user_by_email(db, email=user.email)
    if db_user:
        raise HTTPException(status_code=400, detail="Email already registered")
    db_user = crud.get_user_by_username(db, username=user.username)
    if db_user:
        raise HTTPException(status_code=400, detail="Username already registered")
    return crud.create_user(db=db, user=user)


@app.get("/answers", tags=["Data"])
def answers(
        organization: str,
        campaign: str,
        # current_user: schemas.ApiUser = Depends(get_current_active_user),
        db: Session = Depends(get_db)
):
    return crud.get_answers(db, organization=organization, campaign=campaign)
