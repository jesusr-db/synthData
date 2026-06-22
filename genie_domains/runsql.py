import sys, json, subprocess, time
WH="d56091a1171f30ff"; PROFILE="DEFAULT"
def run(stmt, wait="50s"):
    payload=json.dumps({"warehouse_id":WH,"statement":stmt,"wait_timeout":wait,"catalog":"jmrdemo"})
    p=subprocess.run(["databricks","api","post","/api/2.0/sql/statements","--profile",PROFILE,"--json",payload],
                     capture_output=True,text=True)
    try: d=json.loads(p.stdout)
    except: return ("PARSE_ERR", p.stdout[:300]+p.stderr[:300])
    st=d.get("status",{}).get("state")
    sid=d.get("statement_id")
    # poll if pending
    while st in ("PENDING","RUNNING"):
        time.sleep(2)
        pp=subprocess.run(["databricks","api","get",f"/api/2.0/sql/statements/{sid}","--profile",PROFILE],capture_output=True,text=True)
        d=json.loads(pp.stdout); st=d.get("status",{}).get("state")
    if st!="SUCCEEDED":
        return ("FAIL", d.get("status",{}).get("error",{}).get("message","")[:400])
    return ("OK", d.get("result",{}).get("data_array"))
if __name__=="__main__":
    # read statements from file split on \n;;;\n
    txt=open(sys.argv[1]).read()
    stmts=[s.strip() for s in txt.split("\n;;;\n") if s.strip()]
    fails=0
    for i,s in enumerate(stmts):
        status,info=run(s)
        label=s.split("\n")[0][:70]
        if status=="OK": print(f"[{i+1}/{len(stmts)}] OK   {label}")
        else: print(f"[{i+1}/{len(stmts)}] {status} {label}\n      -> {info}"); fails+=1
    print(f"DONE: {len(stmts)-fails}/{len(stmts)} succeeded")
