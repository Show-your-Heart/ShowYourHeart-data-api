
import sys
from app import crud, database, schemas


def create_user(username, email, password):
    Session = database.SessionLocal
    my_session = Session()
    crud.create_user(my_session
                     , user=schemas.ApiUserCreate(email=email, plain_password=password, username=username))


if __name__ == "__main__":
    try:
        create_user(sys.argv[1], sys.argv[2], sys.argv[3])
    except:
        pass
