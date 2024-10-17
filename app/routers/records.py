from fastapi import APIRouter, Request, Response, HTTPException
from ..validate_record import RecordValidation
from ..database import map_dbspec, post_to_elastic
try:
    from schema.model import PerfsonarLookupServiceSchema
except Exception:
    PerfsonarLookupServiceSchema = dict

router = APIRouter()
validation = RecordValidation()

@router.post("/record/")
def register_record(request: Request, response: Response, registration_record: dict):

    registration_record = validation.validate_record(registration_record)

    if not registration_record['validated']:
        raise HTTPException(status_code=422, 
                            detail="Registration record Validation failed. {}".format(registration_record['error'].message))
    
    registration_record = map_dbspec(registration_record['record'])
    registration_record = map_dbspec(registration_record)
    response = post_to_elastic(registration_record)

    return response