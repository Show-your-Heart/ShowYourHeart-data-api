import datetime
from jose import JWTError, jwt
import pandas as pd
from  coopdevsutils import querytodataframe

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



def get_answers(db, organization:str, campaign: str, language: str = None, direct_indicators: bool = True ):
    lang = ("_"+language) if language is not None else ""
    dr = 'and a.is_direct_indicator' if direct_indicators else 'and not a.is_direct_indicator'
    qry = f"""
        with res as (
            select a.id_campaign, a.campaign_name, a.campaign_name_en, a.campaign_name_ca, a.campaign_name_es, a.campaign_name_eu, a.campaign_name_gl, a.campaign_name_nl, a."year", a.previous_campaign_id
                , a.id_survey, a.survey_created_at, a.survey_updated_at, a.status
                , a.id_method, a.active, a.method_name, a.method_name_en, a.method_name_ca, a.method_name_es, a.method_name_eu, a.method_name_gl, a.method_name_nl, a.method_description, a.method_description_en, a.method_description_ca, a.method_description_es, a.method_description_eu, a.method_description_gl, a.method_description_nl, a.id_user, a.user_name, a.user_surname, a.user_email, a.id_organization, a.organization_name, a.vat_number
                , coalesce(a.id_methods_section, 'e2ef801f-adbc-60d2-36d0-0b9f3516ebc7') id_methods_section, a.method_section_title, a.method_section_title_en, a.method_section_title_ca, a.method_section_title_es, a.method_section_title_eu, a.method_section_title_gl, a.method_section_title_nl, a.method_order, a.method_level, a.path_order, a.sort_value
                , a.id_indicator, a.indicator_code, a.indicator_name, a.indicator_name_en, a.indicator_name_ca, a.indicator_name_es, a.indicator_name_eu, a.indicator_name_gl, a.indicator_name_nl, a.indicator_description, a.indicator_description_en, a.indicator_description_ca, a.indicator_description_es, a.indicator_description_eu, a.indicator_description_gl, a.indicator_description_nl, a.is_direct_indicator, a.indicator_category, a.indicator_data_type, a.indicator_unit, a.gender
                , a.value, a.num_gender, a.str_gender
                , a.str_value, a.str_value_en, a.str_value_ca, a.str_value_es, a.str_value_eu, a.str_value_gl, a.str_value_nl
                , p.gender as prev_gender
                , p.value as prev_value
                , p.str_gender as prev_str_gender
                , p.str_value as prev_str_value, p.str_value_en as prev_str_value_en, p.str_value_ca as prev_str_value_ca, p.str_value_es as prev_str_value_es, p.str_value_eu as prev_str_value_eu, p.str_value_gl as prev_str_value_gl, p.str_value_nl as prev_str_value_nl
                from external.answers_calc_agg a
                left join external.answers_calc_agg p on a.id_organization = p.id_organization and a.previous_campaign_id  = p.id_campaign 
                    and a.id_indicator = p.id_indicator
                where a.id_organization='{organization}'
                and a.id_campaign = '{campaign}'	
                {dr}
        )
        , camp as  (select distinct id_campaign, campaign_name{lang}  as campaign_name
            from res)
        , survey as  (select distinct id_campaign, id_survey,survey_created_at, survey_updated_at,status 
            , id_organization, organization_name, vat_number 
            from res)
        , method as  (select distinct id_campaign, id_survey, id_method, active, method_name{lang} as method_name, method_description{lang} as method_description
            from res)	
        , method_section as  (select distinct id_campaign, id_survey, id_method, id_methods_section, method_section_title{lang} as method_section_title, path_order, method_level
            from res
            order by path_order
            )	
        , indicator as  (select distinct id_campaign, id_survey, id_method, id_methods_section, id_indicator, indicator_code, indicator_name{lang} as indicator_name, indicator_description{lang} as indicator_description
            , is_direct_indicator, indicator_category, indicator_data_type, indicator_unit
            from res)	
        , indicator_result as  (select distinct id_campaign, id_survey, id_method, id_methods_section, id_indicator
            , gender, value, str_gender, str_value{lang} as str_value
            , prev_gender, prev_value, prev_str_gender, prev_str_value{lang} as prev_str_value
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
                                                select json_agg(msi ORDER BY indicator_code)
                                                from (
                                                    select id_indicator, indicator_code, indicator_name, indicator_description, is_direct_indicator, indicator_category, indicator_data_type, indicator_unit
                                                    , (
                                                        select json_agg(ir order by gender, prev_gender)
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
    return registries.fetchone()["json_agg"]



def get_review_answers(db, campaign: str, method: str,  language: str = None ):
    lang = ("_"+language) if language is not None else ""
    qry = f"""
        select id_campaign, campaign_name{lang} as campaign_name, "year"
        , id_survey, survey_created_at::timestamp without time zone, survey_updated_at::timestamp without time zone, status
        , id_method, method_name{lang} as method_name, method_description{lang} as method_description
        , id_user, user_name, user_surname, user_email, id_organization, organization_name, vat_number
        , id_methods_section, method_section_title{lang} as method_section_title, method_order, method_level, path_order, sort_value
        , id_indicator, indicator_code, indicator_name{lang} as indicator_name, indicator_description{lang} as indicator_description, indicator_category, indicator_data_type, indicator_unit
        , str_gender, str_value
        from external.answers_calc_agg a
         where 1=1 
         and a.is_direct_indicator
         and a.id_indicator is not null
        and a.id_campaign = '{campaign}'
        and a.id_method ='{method}'
        order by a.id_organization, path_order, indicator_code
    """
    cols = ['id_campaign', 'campaign_name', 'year'
        , 'id_survey', 'survey_created_at', 'survey_updated_at', 'status'
        , 'id_method', 'method_name', 'method_description'
        , 'id_user', 'user_name', 'user_surname', 'user_email', 'id_organization', 'organization_name', 'vat_number'
        , 'id_methods_section', 'method_section_title', 'method_order', 'method_level', 'path_order', 'sort_value'
        , 'id_indicator', 'indicator_code', 'indicator_name', 'indicator_description', 'indicator_category',
            'indicator_data_type', 'indicator_unit'
        , 'str_gender', 'str_value']
    conn = db.bind
    df = querytodataframe(qry, cols, conn)

    with pd.ExcelWriter(f"{df.iloc[1]['campaign_name']}-{df.iloc[1]['method_name']}.xlsx") as writer:
        for x in df['indicator_code'].unique():
            df1 = df.loc[df['indicator_code'] == x
            , ['indicator_name'
                , 'organization_name', 'vat_number', 'user_email'
                , 'survey_created_at', 'survey_updated_at'
                , 'str_gender', 'str_value']]
            df1.to_excel(writer, sheet_name=x, index=False)
        return f"{df.iloc[1]['campaign_name']}-{df.iloc[1]['method_name']}.xlsx"


