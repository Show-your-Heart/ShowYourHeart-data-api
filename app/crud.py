import datetime
from jose import JWTError, jwt

from sqlalchemy.orm import Session
from typing import Optional

from . import models, schemas
from app.utils import get_password_hash, verify_password
from sqlalchemy.sql import text

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



def get_answers(db, organization:str, campaign: str ):

    qry = f"""
        with res as (
            select a.*
            , p.gender as prev_gender, p.value as prev_value, p.str_gender as prev_str_gender, p.str_value as prev_str_value
                from external.data_results_agg a
                left join external.data_results_agg p on a.id_organization = p.id_organization and a.previous_campaign_id  = p.id_campaign 
                    and a.id_indicator = p.id_indicator
                where a.id_organization='{organization}'
                and a.id_campaign = '{campaign}'	
        )
        , camp as  (select distinct id_campaign, campaign_name 
            from res)
        , survey as  (select distinct id_campaign, id_survey,survey_created_at, survey_updated_at,status 
            , id_organization, organization_name, vat_number 
            from res)
        , method as  (select distinct id_campaign, id_survey, id_method, active, method_name, method_description  
            from res)	
        , method_section as  (select distinct id_campaign, id_survey, id_method, id_methods_section, method_section_title, path_order, method_level
            from res
            order by path_order
            )	
        , indicator as  (select distinct id_campaign, id_survey, id_method, id_methods_section, id_indicator, project_id, indicator_name, indicator_description, is_direct_indicator, indicator_category, indicator_data_type, indicator_unit
            from res)	
        , indicator_result as  (select distinct id_campaign, id_survey, id_method, id_methods_section, id_indicator
            , gender, value, str_gender, str_value
            , prev_gender, prev_value, prev_str_gender, prev_str_value
            from res)		
        SELECT json_agg(t) 
        from (
            select c.id_campaign, c.campaign_name 
                , (
                    SELECT json_agg(s) 
                    FROM (
                        SELECT id_survey,survey_created_at, survey_updated_at,status, id_organization, organization_name, vat_number  
                        , (
                            select json_agg(m)
                            from (
                                select id_method, active, method_name, method_description
                                    , (
                                        select json_agg(ms ORDER BY path_order)
                                        from (
                                            select id_methods_section, method_section_title, path_order, method_level
                                            , (
                                                select json_agg(msi)
                                                from (
                                                    select id_indicator, project_id, indicator_name, indicator_description, is_direct_indicator, indicator_category, indicator_data_type, indicator_unit
                                                    , (
                                                        select json_agg(ir)
                                                        from (
                                                            select gender, value,str_gender, str_value
                                                            , prev_gender, prev_value, prev_str_gender, prev_str_value
                                                            from indicator_result ir
                                                            where  msi.id_campaign = ir.id_campaign  and msi.id_survey = ir.id_survey and msi.id_method =ir.id_method and msi.id_methods_section=ir.id_methods_section
                                                                    and msi.id_indicator =ir.id_indicator
                                                        ) ir
                                                    ) results
                                                    from indicator msi
                                                    where  msi.id_campaign = ms.id_campaign  and msi.id_survey = ms.id_survey and msi.id_method =ms.id_method and msi.id_methods_section=ms.id_methods_section
                                                ) msi
                                            ) as indicators
                                            from method_section ms
                                            where  m.id_campaign = ms.id_campaign  and m.id_survey = ms.id_survey and m.id_method =ms.id_method 
                                        ) ms 
                                    ) as method_section
                                from method m
                                where m.id_campaign = su.id_campaign  and m.id_survey = su.id_survey 
                            ) m
                        ) as methods
                        FROM survey su 
                        WHERE c.id_campaign = su.id_campaign
                    ) s
                ) AS surveys
                from camp c
            ) t
    """


    registries = db.execute(text(qry))
    return [dict(zip(registries.keys(), t)) for t in registries]