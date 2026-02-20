
from typing import Any, Callable, Optional, Tuple, List, Dict
import time, random

class RedshiftError(Exception): ...
class RedshiftTransientError(RedshiftError): ...
class RedshiftPermanentError(RedshiftError): ...

BASE=0.5; FACTOR=2.0; MAX_SLEEP=10.0

def _sleep(attempt:int, sleep_func:Callable[[float],None]):
    s=min(MAX_SLEEP, BASE*(FACTOR**attempt))
    s=s*(1+(random.random()-0.5)*0.2)
    sleep_func(max(0.0,s))

def run_statement(client: Any, sql: str, database: str, db_user: str,
                  wait_for_completion: bool=True, timeout_seconds:int=300,
                  sleep_func:Callable[[float],None]=time.sleep,
                  on_retry:Optional[Callable[[Exception,int],None]]=None)->Tuple[bool,str]:
    resp=client.execute_statement(Sql=sql, Database=database, DbUser=db_user)
    sid=resp.get("Id","test-id")
    if not wait_for_completion: return True,sid
    start=time.time(); attempt=0
    while True:
        desc=client.describe_statement(Id=sid)
        status=desc.get("Status","FINISHED")
        if status=="FINISHED": return True,sid
        if status=="FAILED":
            if attempt<3:
                if on_retry: on_retry(Exception("transient"),attempt)
                _sleep(attempt,sleep_func); attempt+=1; continue
            raise RedshiftPermanentError(desc.get("Error","failed"))
        if time.time()-start>timeout_seconds: raise RedshiftTransientError("timeout")
        _sleep(attempt,sleep_func)

def fetch_statement_result(client:Any, statement_id:str, *,
                           sleep_func:Callable[[float],None]=time.sleep,
                           timeout_seconds:int=60)->List[Dict[str,Any]]:
    desc=client.describe_statement(Id=statement_id)
    if desc.get("Status")!="FINISHED": return []
    res=client.get_statement_result(Id=statement_id)
    cols=[c.get("name","col") for c in res.get("ColumnMetadata",[])]
    rows=[]
    for r in res.get("Records",[]):
        rows.append({cols[i]:list(v.values())[0] for i,v in enumerate(r)})
    return rows
